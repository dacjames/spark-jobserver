package spark.jobserver

import akka.actor.Props
import spark.jobserver.io.JobDAOActor

/**
 * This tests JobManagerActor of AdHoc context.
 * Pass true to isAdHoc when the JobManagerActor is created.
 */
class JobManagerActorAdHocSpec extends JobManagerSpec {

  before {
    dao = new InMemoryDAO
    daoActor = system.actorOf(JobDAOActor.props(dao))
    manager =
      system.actorOf(JobManagerActor.props("test", JobManagerSpec.config, true))
  }

}
