package spark.jobserver

import java.util.concurrent.ExecutorService

import java.util.concurrent.Executors._
import akka.actor.{ActorRef, Props, PoisonPill}
import com.typesafe.config.Config
import java.net.{URI, URL}
import java.util.concurrent.atomic.AtomicInteger
import ooyala.common.akka.InstrumentedActor
import org.apache.spark.{ SparkEnv, SparkContext }
import org.joda.time.DateTime
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.control.NonFatal
import scala.concurrent.{ Future, ExecutionContext }
import scala.util.{Failure, Success, Try}
import spark.jobserver.ContextSupervisor.StopContext
import spark.jobserver.io.{JobDAOActor, JobDAO, JobInfo, JarInfo}
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}

object JobManagerActor {
  // Messages
  case class Initialize(daoActor: ActorRef, resultActorOpt: Option[ActorRef],
                         contextName: String, contextConfig: Config, isAdHoc: Boolean, supervisor: ActorRef)
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])
  case object DequeueTick
  case class KillJob(jobId: String)

  // Results/Data
  case class Initialized(resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)

  // Akka 2.2.x style actor props for actor creation
  def props(): Props = Props(classOf[JobManagerActor])
}

/**
 * The JobManager actor supervises jobs running in a single SparkContext, as well as shared metadata.
 * It creates a SparkContext (or a StreamingContext etc. depending on the factory class)
 * It also creates and supervises a JobResultActor and JobStatusActor, although an existing JobResultActor
 * can be passed in as well.
 *
 * == contextConfig ==
 * {{{
 *  num-cpu-cores = 4         # Total # of CPU cores to allocate across the cluster
 *  memory-per-node = 512m    # -Xmx style memory string for total memory to use for executor on one node
 *  dependent-jar-uris = ["local://opt/foo/my-foo-lib.jar"]
 *                            # URIs for dependent jars to load for entire context
 *  context-factory = "spark.jobserver.context.DefaultSparkContextFactory"
 *  spark.mesos.coarse = true  # per-context, rather than per-job, resource allocation
 *  rdd-ttl = 24 h            # time-to-live for RDDs in a SparkContext.  Don't specify = forever
 * }}}
 *
 * == global configuration ==
 * {{{
 *   spark {
 *     jobserver {
 *       max-jobs-per-context = 16      # Number of jobs that can be run simultaneously per context
 *     }
 *   }
 * }}}
 */
class JobManagerActor extends InstrumentedActor {

  import CommonMessages._
  import JobManagerActor._
  import scala.util.control.Breaks._
  import collection.JavaConverters._

  val config = context.system.settings.config
  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxRunningJobs))

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.

  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  private val jobQueueEnabled = Try(config.getBoolean("spark.job-queue.enabled")).getOrElse(false)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)

  //NOTE: Must be initialized after sparkContext is created
  private var jobCache: JobCache = _

  private var statusActor: ActorRef = _
  protected var resultActor: ActorRef = _
  private var daoActor: ActorRef = _
  private var contextName: String = _
  private var contextConfig: Config = _
  private var isAdHoc: Boolean = _
  private var supervisor: ActorRef = _

  import scala.concurrent.duration._
  private val tickDelay = 500.millis

  private val jobQueue =
    collection.mutable.Queue[(JobJarInfo, JobInfo, Config, ActorRef, ContextLike, SparkEnv, ActorRef)]()
  private var queueExecutor: ExecutionContext = null

  override def preStart(): Unit = {
    if (jobQueueEnabled) {
      import java.util.concurrent.Executors
      queueExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
      context.system.scheduler.scheduleOnce(tickDelay, self, DequeueTick)(queueExecutor)
    }
  }

  override def postStop() {
    logger.info("Shutting down SparkContext {}", contextName)
    Option(jobContext).foreach(_.stop())
  }

  def wrappedReceive: Receive = {
    case Initialize(dao, resOpt, ctxName, ctxConf, adHoc, sup) =>
      daoActor = dao
      statusActor = context.actorOf(Props(classOf[JobStatusActor], daoActor))
      resultActor = resOpt.getOrElse(context.actorOf(Props[JobResultActor]))
      contextName = ctxName
      contextConfig = ctxConf
      isAdHoc = adHoc
      supervisor = sup

      try {
        logger.info("recvd initialize")
        // Load side jars first in case the ContextFactory comes from it
        getSideJars(contextConfig).foreach { jarUri =>
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
        }
        jobContext = createContextFromConfig()
        logger.info("created context: " + jobContext)
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(classOf[RddManagerActor], jobContext.sparkContext),
                                          "rdd-manager-actor")
        jobCache = new JobCache(jobCacheSize, daoActor, jobContext.sparkContext, jarLoader, jobCacheEnabled)
        getSideJars(contextConfig).foreach { jarUri => jobContext.sparkContext.addJar(jarUri) }
        logger.info("sending initialized")
        sender ! Initialized(resultActor)
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, rddManagerActor)

    case DequeueTick =>
      context.system.scheduler.scheduleOnce(tickDelay, self, DequeueTick)(queueExecutor)
      if (jobQueue.size > 0) {
        if (currentRunningJobs.getAndIncrement < maxRunningJobs) {
          val queuedJob = jobQueue.dequeue()
          logger.info("Launching queued job {}", queuedJob._2.jobId)
          createJobFuture(queuedJob._1, queuedJob._2, queuedJob._3, queuedJob._4,
            queuedJob._5, queuedJob._6, queuedJob._7)
        } else {
          currentRunningJobs.decrementAndGet()
        }
      }

    case KillJob(jobId: String) => {
      jobContext.sparkContext.cancelJobGroup(jobId)
      statusActor ! JobKilled(jobId, DateTime.now())
    }

  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    breakable {
      import akka.pattern.ask
      import akka.util.Timeout
      import scala.concurrent.duration._
      import scala.concurrent.Await

      val daoAskTimeout = Timeout(3 seconds)
      val resp = Await.result(
        (daoActor ? JobDAOActor.GetLastUploadTime(appName))(daoAskTimeout).mapTo[JobDAOActor.LastUploadTime],
        daoAskTimeout.duration)

      val lastUploadTime = resp.lastUploadTime
      if (!lastUploadTime.isDefined) {
        sender ! NoSuchApplication
        break
      }

      // Check appName, classPath from jar
      val jarInfo = JarInfo(appName, lastUploadTime.get)
      val jobId = java.util.UUID.randomUUID().toString()
      logger.info("Loading class {} for app {}", classPath, appName: Any)
      val jobJarInfo = try {
        jobCache.getSparkJob(jarInfo.appName, jarInfo.uploadTime, classPath)
      } catch {
        case _: ClassNotFoundException =>
          sender ! NoSuchClass
          postEachJob()
          break
          null // needed for inferring type of return value
        case err: Throwable =>
          sender ! JobLoadingError(err)
          postEachJob()
          break
          null
      }

      // Validate that job fits the type of context we launched
      val job = jobJarInfo.constructor()
      if (!jobContext.isValidJob(job)) {
        sender ! WrongJobType
        break
      }

      // Automatically subscribe the sender to events so it starts getting them right away
      resultActor ! Subscribe(jobId, sender, events)
      statusActor ! Subscribe(jobId, sender, events)

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, Some(DateTime.now()), None, None)
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, jobContext, sparkEnv,
                            rddManagerActor, (jarInfo, classPath)))
    }

    future
  }

  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef,
                           jarInfoAndCp: (JarInfo, String)): Future[Any] = {
    val jobId = jobInfo.jobId

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, queue the job, notify the sender of queueing, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      if (jobQueueEnabled && !isAdHoc) {
        jobQueue.enqueue((jobJarInfo, jobInfo, jobConfig, subscriber, jobContext, sparkEnv, rddManagerActor))
        statusActor ! JobQueued(jobId, contextName, DateTime.now(), jarInfoAndCp._1, jarInfoAndCp._2)
        sender ! JobQueued(jobId, contextName, DateTime.now(), jarInfoAndCp._1, jarInfoAndCp._2)
        Future[Any](None)(executionContext)
      } else {
        sender ! NoJobSlotsAvailable(maxRunningJobs)
        Future[Any](None)(executionContext)
      }
    } else {
      createJobFuture(jobJarInfo, jobInfo, jobConfig, subscriber, jobContext, sparkEnv, rddManagerActor)
    }

  }

  private def createJobFuture(jobJarInfo: JobJarInfo, jobInfo: JobInfo, jobConfig: Config,
                              subscriber: ActorRef, jobContext: ContextLike, sparkEnv: SparkEnv,
                              rddManagerActor: ActorRef): Future[Any] = {

    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor

    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobJarInfo.className)

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")

      // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
      SparkEnv.set(sparkEnv)

      // Use the Spark driver's class loader as it knows about all our jars already
      // NOTE: This may not even be necessary if we set the driver ActorSystem classloader correctly
      Thread.currentThread.setContextClassLoader(jarLoader)
      val job = constructor()
      if (job.isInstanceOf[NamedRddSupport]) {
        val namedRdds = job.asInstanceOf[NamedRddSupport].namedRddsPrivate
        if (namedRdds.get() == null) {
          namedRdds.compareAndSet(null, new JobServerNamedRdds(rddManagerActor))
        }
      }

      try {
        statusActor ! JobStatusActor.JobInit(jobInfo)

        val jobC = jobContext.asInstanceOf[job.C]
        job.validate(jobC, jobConfig) match {
          case SparkJobInvalid(reason) => {
            val err = new Throwable(reason)
            statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
            throw err
          }
          case SparkJobValid => {
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime.get)
            val sc = jobContext.sparkContext
            sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
            job.runJob(jobC, jobConfig)
          }
        }
      }

      catch {
        case NonFatal(e) => throw e
        case e: Throwable => throw new Exception("wrapped fatal", e)
      }

      finally {
        org.slf4j.MDC.remove("jobId")
      }
    }(executionContext).andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, DateTime.now())
        resultActor ! JobResult(jobId, result)
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.warn("Exception from job " + jobId + ": ", error)
    }(executionContext).andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }(executionContext)
  }

  // Use our classloader and a factory to create the SparkContext.  This ensures the SparkContext will use
  // our class loader when it spins off threads, and ensures SparkContext can find the job and dependent jars
  // when doing serialization, for example.
  def createContextFromConfig(contextName: String = contextName): ContextLike = {
    val factoryClassName = contextConfig.getString("context-factory")
    val factoryClass = jarLoader.loadClass(factoryClassName)
    val factory = factoryClass.newInstance.asInstanceOf[spark.jobserver.context.SparkContextFactory]
    Thread.currentThread.setContextClassLoader(jarLoader)
    factory.makeContext(config, contextConfig, contextName)
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete the JobManagerActor after each adhoc job
    if (isAdHoc) supervisor ! StopContext(contextName)
  }

  // Protocol like "local" is supported in Spark for Jar loading, but not supported in Java.
  // This method helps convert those Spark URI to those supported by Java.
  // "local" URIs means that the jar must be present on each job server node at the path,
  // as well as on every Spark worker node at the path.
  // For the job server, convert the local to a local file: URI since Java URI doesn't understand local:
  private def convertJarUriSparkToJava(jarUri: String): String = {
    val uri = new URI(jarUri)
    uri.getScheme match {
      case "local" => "file://" + uri.getPath
      case _ => jarUri
    }
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] =
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq).
     orElse(Try(config.getString("dependent-jar-uris").split(",").toSeq)).getOrElse(Nil)
}
