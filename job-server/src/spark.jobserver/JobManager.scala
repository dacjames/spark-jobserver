package spark.jobserver

import java.io.File

import akka.actor.{ActorSystem, Address, Props}
import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

/**
 * Created by ankits on 3/24/15.
 */
object JobManager {

  val logger = LoggerFactory.getLogger(getClass)

  // Allow custom function to create ActorSystem.  An example of why this is useful:
  // we can have something that stores the ActorSystem so it could be shut down easily later.
  def start(args: Array[String], makeSystem: Config => ActorSystem) {
    val managerConfig = ConfigFactory.load("jobmanager.conf").withFallback(ConfigFactory.load())
    val contextName = if (args.length > 0) args(0) else java.util.UUID.randomUUID.toString
    val config = if (args.length > 1) {
      val configFile = new File(args(1))
      if (!configFile.exists()) {
        println("Could not find configuration file " + configFile)
        sys.exit(1)
      }
      ConfigFactory.parseFile(configFile).withFallback(managerConfig)
    } else {
      managerConfig
    }
    println("Starting JobManager with config " + config.getConfig("spark").root.render)
    logger.info("Starting JobManager with config {}", config.getConfig("spark").root.render())

    val system = makeSystem(config)
    //Cluster(system).join//join(Address("akka.tcp", "JobServer", "127.0.0.1", 2551))

    val defaultContextConfig = config.getConfig("spark.context-settings")
    val jobManager = system.actorOf(Props(classOf[JobManagerActor], contextName,
      defaultContextConfig, false), "jobManager")

    val cluster = Cluster(system)
    logger.info("JobManager system: " + system)

    //val jarManager = system.actorOf(Props(classOf[JarManager], jobDAO), "jar-manager")
    //val supervisor = system.actorOf(Props(classOf[AkkaClusterSupervisor], jobDAO), "context-supervisor")
    //val jobInfo = system.actorOf(Props(classOf[JobInfoActor], jobDAO, supervisor), "job-info")



    // Create initial contexts
    //supervisor ! ContextSupervisorMessages.AddContextsFromConfig
    //new WebApi(system, config, port, jarManager, supervisor, jobInfo).start()

  }

  def main(args: Array[String]) {
    start(args, config => ActorSystem("JobServer", config))
  }
}
