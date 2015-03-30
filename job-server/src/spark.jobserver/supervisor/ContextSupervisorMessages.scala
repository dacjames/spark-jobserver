package spark.jobserver.supervisor

import com.typesafe.config.Config

/**
 * Created by ankits on 3/16/15.
 */
/** Messages common to all ContextSupervisors */
object ContextSupervisorMessages {
  // Messages/actions
  case object AddContextsFromConfig // Start up initial contexts
  case object ListContexts
  case class AddContext(name: String, contextConfig: Config)
  case class GetAdHocContext(classPath: String, contextConfig: Config)
  case class GetContext(name: String) // returns JobManager, JobResultActor
  case class GetResultActor(name: String)  // returns JobResultActor
  case class StopContext(name: String)

  // Errors/Responses
  case object ContextInitialized
  case class ContextInitError(t: Throwable)
  case object ContextAlreadyExists
  case object NoSuchContext
  case object ContextStopped
}