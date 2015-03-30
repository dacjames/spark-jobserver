package spark.jobserver

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import org.joda.time.DateTime
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.io.JobDAOActor._
import spark.jobserver.supervisor.ContextSupervisorMessages

import scala.concurrent.Await

object JobInfoActor {
  // Requests
  case class GetJobStatuses(limit: Option[Int])
  case class GetJobConfig(jobId: String)
  case class StoreJobConfig(jobId: String, jobConfig: Config)

  // Responses
  case object JobConfigStored
}

class JobInfoActor(jobDao: ActorRef, contextSupervisor: ActorRef) extends InstrumentedActor {
  import context.dispatcher
  import spark.jobserver.CommonMessages._
  import spark.jobserver.JobInfoActor._

import scala.concurrent.duration._
  import scala.util.control.Breaks._       // for futures to work

  // Used in the asks (?) below to request info from contextSupervisor and resultActor
  implicit val ShortTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case GetJobStatuses(limit) =>
      val infoReq = (jobDao ? GetJobInfos).mapTo[JobInfos]
      val infos = Await.result(infoReq, 3 seconds).infos.values.toSeq.sortBy(_.startTime.toString())
      if (limit.isDefined) {
        sender ! infos.takeRight(limit.get)
      } else {
        sender ! infos
      }

    case GetJobResult(jobId) =>
      breakable {
        val infoReq = (jobDao ? GetJobInfo(jobId)).mapTo[JobInfoResponse]
        val jobInfoOpt = Await.result(infoReq, 3 seconds).info

        if (!jobInfoOpt.isDefined) {
          sender ! NoSuchJobId
          break
        }

        jobInfoOpt.filter { job => job.isRunning || job.isErroredOut }
          .foreach { jobInfo =>
            sender ! jobInfo
            break
          }

        // get the context from jobInfo
        val context = jobInfoOpt.get.contextName

        val future = (contextSupervisor ? ContextSupervisorMessages.GetResultActor(context)).mapTo[ActorRef]
        val resultActor = Await.result(future, 3 seconds)

        val receiver = sender // must capture the sender since callbacks are run in a different thread
        for (result <- (resultActor ? GetJobResult(jobId))) {
          receiver ! result // a JobResult(jobId, result) object is sent
        }
      }

    case GetJobConfig(jobId) =>
      val confReq = (jobDao ? GetJobConfig(jobId)).mapTo[JobConfigResponse]
      sender ! Await.result(confReq, 3 seconds).config.getOrElse(NoSuchJobId)

    case StoreJobConfig(jobId, jobConfig) =>
      jobDao ! SaveJobConfig(jobId, jobConfig)
      sender ! JobConfigStored
  }
}
