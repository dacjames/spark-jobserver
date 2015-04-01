package spark.jobserver

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import ooyala.common.akka.actor.ProductionReaper
import ooyala.common.akka.actor.Reaper.WatchMe
import org.slf4j.LoggerFactory

/**
 * Created by ankits on 3/24/15.
 */
object JobManager {

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
    val contextName = if (args.length > 1) args(1) else java.util.UUID.randomUUID.toString
    logger.info("Starting JobManager context " + contextName + " with config {}",
      config.getConfig("spark").root.render())

    val system = makeSystem(config)
    val defaultContextConfig = config.getConfig("spark.context-settings")
    val jobManager = system.actorOf(Props(classOf[JobManagerActor], contextName,
      defaultContextConfig, false), "jobManager")

    //Kill process when jobmanager is shutdown.
    val reaper = system.actorOf(Props[ProductionReaper])
    reaper ! WatchMe(jobManager)
    system.registerOnTermination(System.exit(0))
  }

  def main(args: Array[String]) {
    start(args, config => ActorSystem("JobServer", config))
  }
}
