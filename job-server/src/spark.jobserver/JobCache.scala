package spark.jobserver

import java.net.URL

import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import scala.concurrent.duration._
import spark.jobserver.io.JobDAOActor.{GetJarPath, JarPath}
import spark.jobserver.util.{ContextURLClassLoader, JarUtils, LRUCache}

case class JobJarInfo(constructor: () => SparkJobBase,
                      className: String,
                      jarFilePath: String)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */
class JobCache(maxEntries: Int, dao: ActorRef, sparkContext: SparkContext, loader: ContextURLClassLoader,
                jarCacheEnabled: Boolean) {
  private val cache = new LRUCache[(String, DateTime, String), JobJarInfo](maxEntries)
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val askTimeout = Timeout(10 seconds)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    if (jarCacheEnabled)
    {
      logger.info(s"JobCache: Jar caching enabled for ${(appName, uploadTime, classPath)}")
      cache.get((appName, uploadTime, classPath), loadJar(appName, uploadTime, classPath))
    }
    else
    {
      logger.info(s"JobCache: Jar caching disabled for ${(appName, uploadTime, classPath)}")
      loadJar(appName, uploadTime, classPath)
    }
  }

  private def loadJar(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    import akka.pattern.ask
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val askJarPath = (dao ? GetJarPath(appName, uploadTime))
    val jarPath = Await.result(askJarPath,5 seconds).asInstanceOf[JarPath].path
    val jarFilePath = new java.io.File(jarPath).getAbsolutePath
    sparkContext.addJar(jarFilePath) // Adds jar for remote executors
    loader.addURL(new URL("file:" + jarFilePath)) // Now jar added for local loader
    val constructor = JarUtils.loadClassOrObject[SparkJob](classPath, loader)
    JobJarInfo(constructor, classPath, jarFilePath)
  }
}
