package spark.jobserver

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import spark.jobserver.io.{JobDAO, JobDAOActor}
import spark.jobserver.supervisor._

/**
 * The Spark Job Server is a web service that allows users to submit and run Spark jobs, check status,
 * and view results.
 * It may offer other goodies in the future.
 * It only takes in one optional command line arg, a config file to override the default (and you can still
 * use -Dsetting=value to override)
 * -- Configuration --
 * {{{
 *   spark {
 *     master = "local"
 *     jobserver {
 *       port = 8090
 *     }
 *   }
 * }}}
 */
object JobServer {
  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val baseConfig = ConfigFactory.load()
    val config = if (args.length > 0) {
      val configFile = new File(args(0))
      if (!configFile.exists()) {
        println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(baseConfig)
    } else {
      baseConfig
    }
    logger.info("Starting JobServer with config {}", config.getConfig("spark").root.render())
    val port = config.getInt("spark.jobserver.port")

    // TODO: Hardcode for now to get going. Make it configurable later.
    val system = makeSystem(config)
    //Cluster(system).join(Address("akka.tcp", "JobServer", "127.0.0.1", 2551))
    val clazz = Class.forName(config.getString("spark.jobserver.jobdao"))
    val ctor = clazz.getDeclaredConstructor(Class.forName("com.typesafe.config.Config"))
    val jobDAO = ctor.newInstance(config).asInstanceOf[JobDAO]

    val jobDAOActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO))
    val jarManager = system.actorOf(Props(classOf[JarManager], jobDAOActor), "jar-manager")
    val supervisor = system.actorOf(Props(classOf[AkkaClusterSupervisor], jobDAOActor), "context-supervisor")
    val jobInfo = system.actorOf(Props(classOf[JobInfoActor], jobDAOActor, supervisor), "job-info")



    // Create initial contexts
    supervisor ! ContextSupervisorMessages.AddContextsFromConfig
    new WebApi(system, config, port, jarManager, supervisor, jobInfo).start()

  }

  def main(args: Array[String]) {
    start(args, config => ActorSystem("JobServer", config))
  }
}
