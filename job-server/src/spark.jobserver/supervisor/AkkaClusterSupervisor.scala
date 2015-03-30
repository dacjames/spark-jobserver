package spark.jobserver.supervisor

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{ClusterEvent, Cluster, Member}
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.JobManagerActor.{ContextInfo, GetContextInfo}
import spark.jobserver.util.SparkJobUtils
import spark.jobserver.{JobManagerActor, JobResultActor, JobStatusActor}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class AkkaClusterSupervisor(daoActor: ActorRef) extends InstrumentedActor {
  import spark.jobserver.supervisor.ContextSupervisorMessages._

import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextTimeout = SparkJobUtils.getContextTimeout(config)

  implicit val resolveTimeout = Timeout(10 seconds)
  import context.dispatcher   // to get ExecutionContext for futures

  private val contexts = mutable.HashMap.empty[String, ActorRef]
  private val resultActors = mutable.HashMap.empty[String, ActorRef]
  private val statusActors = mutable.HashMap.empty[String, ActorRef]

  private val memberInitializations = mutable.HashMap.empty[ActorRef, ActorRef => Unit]

  private val cluster = Cluster(context.system)

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  private def dropMember(member:Member) = {
    context.actorSelection(RootActorPath(member.address) / "user" / "jobManager").resolveOne().onSuccess({
      case ref =>
        contexts.foreach { kv => if (kv._2 == ref) contexts.remove(kv._1) }
        resultActors.foreach { kv => if (kv._2 == ref) resultActors.remove(kv._1) }
        statusActors.foreach { kv => if (kv._2 == ref) statusActors.remove(kv._1) }
    })
  }

  //joining, up, leaving, exiting, down
  def wrappedReceive: Receive = {
    //Cluster events
    case MemberUp(member) =>
      logger.info("ACS member up: " + member)
      logger.info("member roles: " + member.getRoles )
      if (member.hasRole("jobManager")) {
        context.actorSelection(RootActorPath(member.address)/"user"/"jobManager").resolveOne().onComplete {
          case Success(ref) =>
            val statusActor = context.actorOf(Props(classOf[JobStatusActor], daoActor))
            val resultActor = context.actorOf(Props[JobResultActor])
            (ref ? JobManagerActor.Initialize(daoActor, statusActor, resultActor)).onComplete { t => t match
              {
                case Failure(e: Exception) =>
                  logger.error("Excepting initializing JobManagerActor: " + ref, e)
                  ref ! PoisonPill
                case Success(JobManagerActor.Initialized) =>
                  (ref ? GetContextInfo).onComplete {
                    case Failure(e) =>
                      logger.error("Exception getting context info for JobManagerActor: " + ref, e)
                      ref ! PoisonPill
                    case Success(ContextInfo(ctxName, ctxConf, ctxIsAdHoc)) =>
                      if (contexts contains ctxName)
                      {
                        logger.error("JobManager with context " + ctxName + " joined, but" +
                          " context with that name exists")
                      }
                      else
                      {
                        logger.info("SparkContext {} joined", ctxName)
                        contexts(ctxName) = ref
                        statusActors(ctxName) = statusActor
                        resultActors(ctxName) = resultActor
                      }
                    case x =>
                      logger.warn("Unexpected reply to get context info: {}", x)
                      ref ! PoisonPill
                  }
                case Success(JobManagerActor.InitError(t)) =>
                  logger.warn("Jobmanager init error {}", t)
                  ref ! PoisonPill
                case x =>
                  logger.warn("Unexpected message received from job manager: {}", x)
              }
            }
          case Failure(e) =>
            logger.warn("Unable to find job manager actor on cluster member " + member.address, e)
        }
      }

    case MemberExited(member) =>
      if (member.hasRole("jobManager")) dropMember(member)
    case MemberRemoved(member, prevStatus) =>
      if (member.hasRole("jobManager")) dropMember(member)

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      logger.info("AkkaClusterSupervisor received ListContexts")
      sender ! contexts.keys.toSeq

    case AddContext(name, contextConfig) =>
      val originator = sender // Sender is a mutable reference, must capture in immutable val
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (contexts contains name) {
        originator ! ContextAlreadyExists
      } else {
        startContext(name, mergedConfig, false, contextTimeout) { contextMgr =>
          originator ! ContextInitialized
        } { err =>
          originator ! ContextInitError(err)
        }
      }

    case GetAdHocContext(classPath, contextConfig) =>
      val originator = sender // Sender is a mutable reference, must capture in immutable val
      logger.info("Creating SparkContext for adhoc jobs.")

      val mergedConfig = contextConfig.withFallback(defaultContextConfig)

      // Keep generating context name till there is no collision
      var contextName = ""
      do {
        contextName = java.util.UUID.randomUUID().toString().substring(0, 8) + "-" + classPath
      } while (contexts contains contextName)

      // Create JobManagerActor and JobResultActor
      startContext(contextName, mergedConfig, true, contextTimeout) { contextMgr =>
        originator ! (contexts(contextName), resultActors(contextName))
      } { err =>
        originator ! ContextInitError(err)
      }

    case GetResultActor(name) =>
      sender ! resultActors.get(name).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! (contexts(name), resultActors(name))
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)

        context.watch(contexts(name))
        contexts(name) ! PoisonPill
        resultActors.remove(name)
        statusActors.remove(name)
        sender ! ContextStopped
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name :String = actorRef.path.name
      logger.info("Actor terminated: " + name)
      contexts.foreach { kv => if (kv._2 == actorRef) contexts.remove(kv._1) }
  }

  override def postStop():Unit = {
    cluster.unsubscribe(self)
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean, timeoutSecs: Int = 1)
                        (successFunc: ActorRef => Unit)
                        (failureFunc: Throwable => Unit): Unit = {
    require(!(contexts contains name), "There is already a context named "  + name)
    logger.info("Creating a SparkContext named {} remotely", name)

    //def remoteScope(): RemoteScope = RemoteScope(Address("akka.tcp", "JobServer", "127.0.0.1", 2551))

    //val ref = context.system.actorOf(Props(
    //  classOf[JobManagerActor], name, contextConfig, isAdHoc).
    //    withDeploy(Deploy(scope = remoteScope())), name)

    import java.io.File
    val seperator = File.separator; //System.getProperty("file.seperator")
    val classpath = System.getProperty("java.class.path")
    val javaHome = System.getProperty("java.home")
    val javaPath = javaHome + seperator + "bin" + seperator + "java"

    import spark.jobserver.JobManager
    logger.info("Launching process: {}", javaPath + "-cp " + classpath)
    val pb = new ProcessBuilder(javaPath, "-cp", classpath, JobManager.getClass.getName)
    val process = pb.start()
    val is = process.getInputStream
    val es = process.getErrorStream
    Thread.sleep(30 * 1000)

    logger.info("process stdout after 30 secs: {} ", scala.io.Source.fromInputStream(is).mkString)
    logger.info("process stderr after 30 secs: {} ", scala.io.Source.fromInputStream(es).mkString)


//    memberInitializations(ref) = {
//      case Failure(e: Exception) =>
//        logger.error("Exception after sending Initialize to JobManagerActor", e)
//        // Make sure we try to shut down the context in case it gets created anyways
//        ref ! PoisonPill
//        failureFunc(e)
//      case Success(JobManagerActor.Initialized(_)) =>
//        logger.info("SparkContext {} initialized", name)
//        contexts(name) = ref
//        resultActors(name) = resultActorRef
//        statusActors(name) = statusActorRef
//        successFunc(ref)
//      case Success(JobManagerActor.InitError(t)) =>
//        ref ! PoisonPill
//        failureFunc(t)
//      case x =>
//        logger.warn("Unexpected message received from job manager: {}", x)
//    }
//
//    val defaultInitialization: Try[Any] => Unit = {
//      case Failure(e: Exception) =>
//        logger.error("Excepting initializing JobManagerActor: " + ref, e)
//        ref ! PoisonPill
//      case Success(JobManagerActor.Initialized(_)) =>
//        (ref ? GetContextInfo)(contextTimeout).onComplete {
//          case Failure(e) =>
//            logger.error("Exception getting context info for JobManagerActor: " + ref, e)
//            ref ! PoisonPill
//          case Success(ContextInfo(ctxName, ctxConf, ctxIsAdHoc, ctxResActorOpt, ctxStatusActorOpt)) =>
//            if (contexts contains ctxName)
//            {
//              logger.error("JobManager with context " + ctxName + " joined, " +
//                "but context with that name exists")
//            }
//           else
//            {
//              logger.info("SparkContext {} joined", ctxName)
//              contexts(ctxName) = ref
//              resultActors(ctxName) = ctxResActorOpt.getOrElse(context.actorOf(Props[JobResultActor],
//                s"result-actor-$ctxName"))
//              statusActors(ctxName) = ctxStatusActorOpt.getOrElse(context.actorOf(Props
//                (classOf[JobStatusActor], dao), s"status-actor-$name"))
//            }
//        }
//      case Success(JobManagerActor.InitError(t)) =>
//        ref ! PoisonPill
//      case x =>
//        logger.warn("Unexpected message received from job manager: {}", x)
//    }


  }

  // Adds the contexts from the config file
  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false, contextTimeout) { ref => } {
          e => logger.error("Unable to start context " + contextName, e)
        }
        Thread sleep 500 // Give some spacing so multiple contexts can be created
      }
    }
  }

}
