package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventSeq, ReadEventAdapter}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object EventAdapters extends App {

  // online store for acoustic guitars
  val ACOUSTIC = "acoustic"
  val ELECTRIC = "electric"

  // data structures
  case class Guitar(id: String, model: String, make: String, guitarType: String = ACOUSTIC)
  // command
  case class AddGuitar(guitar: Guitar, quantity: Int)
  // event
  case class GuitarAdded(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int)
  case class GuitarAddedV2(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int, guitarType: String)

  class InventoryManager extends PersistentActor with ActorLogging {
    override def persistenceId: String = "guitar-inventory-manager"

    val inventory: mutable.Map[Guitar, Int] = new mutable.HashMap[Guitar, Int]()

    override def receiveCommand: Receive = {
      case AddGuitar(guitar @ Guitar(id, model, make, guitarType), quantity) =>
        persist(GuitarAddedV2(id, model, make, quantity, guitarType)) { e =>
          addGuitarInventory(guitar, quantity)
          log.info(s"Added $quantity x $guitar to inventory")
        }
      case "print" =>
        log.info(s"Current inventory is: $inventory")
    }

    override def receiveRecover: Receive = {
//      case event @ GuitarAdded(id, model, make, quantity) =>
//        addGuitarInventory(Guitar(id, model, make), quantity)
//        log.info(s"Recovered $event")
      case event @ GuitarAddedV2(id, model, make, quantity, guitarType) =>
        addGuitarInventory(Guitar(id, model, make, guitarType), quantity)
        log.info(s"Recovered $event")
    }

    def addGuitarInventory(guitar: Guitar, quantity: Int): Unit = {
      val existingQuantity = inventory.getOrElse(guitar, 0)
      inventory.put(guitar, existingQuantity + quantity)
    }
  }

  class GuitarReadEventAdapter extends ReadEventAdapter {
    /*
      journal -> serializer -> read event adapter -> actor
     */
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case GuitarAdded(guitarId, guitarModel, guitarMake, quantity) =>
        EventSeq.single(GuitarAddedV2(guitarId, guitarModel, guitarMake, quantity, ACOUSTIC))
      case event => EventSeq.single(event)
    }
  }
  // WriteEventAdapter - used for backwards compatibility
  // actor -> write event adapter -> serializer -> journal
  // EventAdapter - does both read and write

  val system = ActorSystem("eventAdapters", ConfigFactory.load().getConfig("eventAdapters"))
  val inventoryManager = system.actorOf(Props[InventoryManager], "inventoryManager")

//  val guitars = for (i <- 1 to 10) yield Guitar(s"$i", s"Hakker $i", "RockTheJvm", ELECTRIC)
//  guitars.foreach { guitar =>
//    inventoryManager ! AddGuitar(guitar, 5)
//  }

  inventoryManager ! "print"

}
