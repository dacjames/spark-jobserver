package spark.jobserver

import akka.actor.ActorRef
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import org.joda.time.DateTime
import spark.jobserver.util.JarUtils

// Messages to JarManager actor
case class StoreJar(appName: String, jarBytes: Array[Byte])
case object ListJars

// Responses
case object InvalidJar
case object JarStored

/**
 * An Actor that manages the jars stored by the job server.   It's important that threads do not try to
 * load a class from a jar as a new one is replacing it, so using an actor to serialize requests is perfect.
 */
class JarManager(jobDaoActor: ActorRef) extends InstrumentedActor {
  import akka.pattern.ask
  import spark.jobserver.io.JobDAOActor._

import scala.concurrent.duration._

  implicit val askTimeout = Timeout(10 seconds)

  override def wrappedReceive: Receive = {
    case ListJars => sender ! createJarsList()

    case StoreJar(appName, jarBytes) =>
      logger.info("Storing jar for app {}, {} bytes", appName, jarBytes.size)
      if (!JarUtils.validateJarBytes(jarBytes)) {
        sender ! InvalidJar
      } else {
        val uploadTime = DateTime.now()
        jobDaoActor ! SaveJar(appName, uploadTime, jarBytes)
        sender ! JarStored
      }
  }

  private def createJarsList() = {
    import scala.concurrent.Await
    import scala.concurrent.duration._
    Await.result((jobDaoActor ? GetApps).mapTo[Apps], 3 seconds).apps
  }
}
