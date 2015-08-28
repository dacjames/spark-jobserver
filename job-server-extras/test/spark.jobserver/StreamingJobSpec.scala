package spark.jobserver

import com.typesafe.config.{ConfigValueFactory, ConfigFactory}
import akka.testkit.TestProbe
import spark.jobserver.context.StreamingContextFactory
import spark.jobserver.io.{JobDAOActor, JobInfo}
import spark.jobserver.context.StreamingContextFactory

/**
 * Test for Straming Jobs.
 */
object StreamingJobSpec extends JobSpecConfig {
  override val contextFactory = classOf[StreamingContextFactory].getName
}

class StreamingJobSpec extends JobSpecBase(StreamingJobSpec.getNewSystem) {

  import CommonMessages._

  import collection.JavaConverters._
  import scala.concurrent.duration._

  val classPrefix = "spark.jobserver."
  private val streamingJob = classPrefix + "StreamingTestJob"

  val configMap = Map("streaming.batch_interval" -> Integer.valueOf(3))

  val emptyConfig = ConfigFactory.parseMap(configMap.asJava)
  var jobId = ""

  val streamingContextConfig = JobManagerSpec.config.withValue("context-factory", ConfigValueFactory.fromAnyRef(StreamingJobSpec.contextFactory))

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager = system.actorOf(JobManagerActor.props())
    supervisor = TestProbe().ref
    // system.actorOf(Props(classOf[JobManagerActor], dao, "c1", "local[4]", config, false))
  }

  describe("Spark Streaming Jobs") {
    it("should be able to process data usign Streaming jobs") {
      manager ! JobManagerActor.Initialize(daoActor, None, "ctx", streamingContextConfig, false, supervisor)
      expectMsgClass(10 seconds, classOf[JobManagerActor.Initialized])
      uploadTestJar()
      manager ! JobManagerActor.StartJob("demo", streamingJob, emptyConfig, asyncEvents ++ errorEvents)

      jobId = expectMsgPF(6 seconds, "Did not start StreamingTestJob, expecting JobStarted") {
        case JobStarted(jobid, _, _) => {
          jobid should not be null
          jobid
        }
      }
      Thread sleep 1000
      dao.getJobInfos.get(jobId).get match  {
        case JobInfo(_, _, _, _, _, None, _) => {  }
        case e => fail("Unexpected JobInfo" + e)
      }
    }
  }
}
