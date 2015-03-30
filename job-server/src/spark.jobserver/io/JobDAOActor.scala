package spark.jobserver.io

import com.typesafe.config.Config
import ooyala.common.akka.InstrumentedActor
import org.joda.time.DateTime

/**
 * Created by ankits on 3/25/15.
 */

object JobDAOActor {
  //requests
  case class SaveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte])
  case class GetJarPath(appName: String, uploadTime: DateTime)
  case object GetApps

  case class SaveJobInfo(jobInfo: JobInfo)
  case object GetJobInfos
  case class GetJobInfo(jobId: String)

  case class SaveJobConfig(jobId: String, jobConfig: Config)
  case object GetJobConfigs
  case class GetJobConfig(jobId: String)

  case class GetLastUploadTime(appName:String)


  //responses
  case class JarPath(path:String)
  case class Apps(apps:Map[String,DateTime])

  case class JobInfos(infos: Map[String,JobInfo])
  case class JobInfoResponse(info: Option[JobInfo])

  case class JobConfigs(configs:Map[String, Config])
  case class JobConfigResponse(config: Option[Config])

  case class LastUploadTime(uploadTime:Option[DateTime])



}

class JobDAOActor(dao: JobDAO) extends InstrumentedActor {
  import spark.jobserver.io.JobDAOActor._

  override def wrappedReceive: Receive = {
    case SaveJar(appName, uploadTime, jarBytes) =>
      dao.saveJar(appName,uploadTime,jarBytes)

    case GetJarPath(appName, uploadTime) =>
      sender ! JarPath(dao.retrieveJarFile(appName,uploadTime))

    case GetApps =>
      sender ! Apps(dao.getApps)


    case SaveJobInfo(jobInfo) =>
      dao.saveJobInfo(jobInfo)

    case GetJobInfos =>
      sender ! JobInfos(dao.getJobInfos)

    case GetJobInfo(jobId) =>
      sender ! JobInfoResponse(dao.getJobInfos.get(jobId))


    case SaveJobConfig(jobId, jobConfig) =>
      dao.saveJobConfig(jobId,jobConfig)

    case GetJobConfigs =>
      sender ! JobConfigs(dao.getJobConfigs)

    case GetJobConfig(jobId) =>
      sender ! JobConfigResponse(dao.getJobConfigs.get(jobId))

    case GetLastUploadTime(appName) =>
      sender ! LastUploadTime(dao.getLastUploadTime(appName))

  }


}