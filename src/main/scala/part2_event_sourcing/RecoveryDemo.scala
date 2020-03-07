package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

object RecoveryDemo extends App {

  case class Command(contents: String)
  case class Event(contents: String)

  class RecoveryActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "recovery-actor"

    override def receiveCommand: Receive = {
      case Command(contents) =>
        persist(Event(contents)) { event =>
          log.info(s"Successfully persisted $event")
        }
    }

    override def receiveRecover: Receive = {
      case Event(contents) =>
        if(contents.contains("314"))
          throw new RuntimeException("I can't take this anymore!")
        log.info(s"Recovered: $contents")
    }

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      log.warning("I failed at recovery")
      super.onRecoveryFailure(cause, event)
    }
  }

  val system = ActorSystem("RecoveryDemo")
  val recoveryActor = system.actorOf(Props[RecoveryActor], "recoveryActor")
  /*
    1. Stashing commands
   */
//  for(i <- 1 to 1000) {
//    recoveryActor ! Command(s"Command $i")
//  }
  // ALL COMMANDS SENT DURING RECOVERY ARE STASHED

  /*
    2. Failure during recovery
   */

}
