package spark.jobserver

import java.net.{URI, URL}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import org.apache.spark.SparkEnv
import org.joda.time.DateTime
import spark.jobserver.io.JobDAOActor.{GetLastUploadTime, LastUploadTime}
import spark.jobserver.io.{JarInfo, JobInfo}
import spark.jobserver.supervisor.ContextSupervisorMessages.StopContext
import spark.jobserver.util.{ContextURLClassLoader, SparkJobUtils}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object JobManagerActor {
  // Messages
  case class Initialize(daoActor: ActorRef, resultActor: ActorRef)
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])
  case object GetContextInfo

  // Results/Data
  case object Initialized
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
  case class ContextInfo(name: String, config: Config, isAdHoc: Boolean)

  // Akka 2.2.x style actor props for actor creation
  def props(name: String, config: Config, isAdHoc: Boolean): Props =
    Props(classOf[JobManagerActor], name, config, isAdHoc)

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
class JobManagerActor(contextName: String,
                      contextConfig: Config,
                      isAdHoc: Boolean) extends InstrumentedActor {
  logger.info("JobManager actor inited: " + self)
  import context.dispatcher
  import spark.jobserver.CommonMessages._
  import spark.jobserver.JobManagerActor._

import scala.collection.JavaConverters._
  import scala.util.control.Breaks._       // for futures to work

  val config = context.system.settings.config

  var jobContext: ContextLike = _
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val maxRunningJobs = SparkJobUtils.getMaxRunningJobs(config)
  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.
  // NOTE: It's important that jobCache be lazy as sparkContext is not initialized until later
  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  private val jobCacheEnabled = Try(config.getBoolean("spark.job-cache.enabled")).getOrElse(false)
  // Use Spark Context's built in classloader when SPARK-1230 is merged.
  private val jarLoader = new ContextURLClassLoader(Array[URL](), getClass.getClassLoader)

  var jobCache: JobCache = _ //
  private var statusActor: ActorRef = _
  private var resultActor: ActorRef = _
  private var daoActor: ActorRef = _

  private val cluster = Cluster(context.system)

  implicit val ShortTimeout = Timeout(3 seconds)

  def supervisors(): Set[ActorSelection] = {
    val supMembers = cluster.state.members.filter{m => m.hasRole("supervisor") && m.status == MemberStatus.Up}
    supMembers.map { m => context.actorSelection(RootActorPath(m.address) / "user" / "context-supervisor") }
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = initialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop() {
    cluster.unsubscribe(self)
    logger.info("Shutting down SparkContext {}", contextName)
    Option(jobContext).foreach(_.stop())
  }

  def wrappedReceive: Receive = {
    //cluster messages
    //todo: update supervisors list according to cluster messages
    case MemberUp(member) => logger.info("jobmanageractor recvd memberup " + member.address)
    case MemberExited(member) =>
    case MemberRemoved(member, prevStatus) =>

    case Initialize(daoAct, resActor) =>
      daoActor = daoAct
      resultActor = resActor
      statusActor = context.actorOf(Props(classOf[JobStatusActor], daoActor), "status-actor")
      try {
        // Load side jars first in case the ContextFactory comes from it
        getSideJars(contextConfig).foreach { jarUri =>
          jarLoader.addURL(new URL(convertJarUriSparkToJava(jarUri)))
        }
        jobContext = createContextFromConfig()
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(classOf[RddManagerActor], jobContext.sparkContext))
        jobCache = new JobCache(jobCacheSize, daoActor, jobContext.sparkContext, jarLoader, jobCacheEnabled)
        getSideJars(contextConfig).foreach { jarUri => jobContext.sparkContext.addJar(jarUri) }
        sender ! Initialized
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv, rddManagerActor)

    case GetContextInfo =>
      sender ! ContextInfo(contextName, contextConfig, isAdHoc)
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       jobContext: ContextLike,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    import akka.pattern.ask

import scala.concurrent.Await
    import scala.concurrent.duration._
    breakable {
      val req = (daoActor ? GetLastUploadTime(appName))(10 seconds).mapTo[LastUploadTime]
      val lastUploadTime = Await.result(req, 10 seconds).uploadTime
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

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now(), None, None)
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, jobContext, sparkEnv,
                            rddManagerActor))
    }

    future
  }

  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           jobContext: ContextLike,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef): Future[Any] = {
    // Use the SparkContext's ActorSystem threadpool for the futures, so we don't corrupt our own
    implicit val executionContext = sparkEnv.actorSystem

    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor
    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobJarInfo.className)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      return Future[Any](None)
    }

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
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime)
            job.runJob(jobC, jobConfig)
          }
        }
      } finally {
        org.slf4j.MDC.remove("jobId")
      }
    }.andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, DateTime.now())
        resultActor ! JobResult(jobId, result)
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.warn("Exception from job " + jobId + ": ", error)
    }.andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }
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
    if (isAdHoc) supervisors().foreach{ sup => sup ! StopContext(contextName) }
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
    Try(config.getStringList("dependent-jar-uris").asScala.toSeq).getOrElse(Nil)
}
