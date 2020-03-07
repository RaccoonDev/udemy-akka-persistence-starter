package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventAdapter, EventSeq}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

object DomainModel {
  case class User(id: String, email: String)
  case class Coupon(code: String, promotionAmount: Int)

  // command
  case class ApplyCoupon(coupon: Coupon, user: User)
  // event
  case class CouponApplied(code: String, user: User)
}

object DataModel {
  case class WrittenCouponApplied(code: String, userId: String, userEmail: String)
}

class ModelAdaptor extends EventAdapter {
  import DomainModel._
  import DataModel._

  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case WrittenCouponApplied(code, userId, userEmail) =>
      println("converting from journal")
      EventSeq.single(CouponApplied(code, User(userId, userEmail)))
    case other => EventSeq.single(other)
  }

  override def manifest(event: Any): String = "CMA"

  override def toJournal(event: Any): Any = event match {
    case CouponApplied(code, user) =>
      println("converting to journal")
      WrittenCouponApplied(code, user.id, user.email)
    case other => other
  }
}

object DetachingModels extends App {

  class CouponManager extends PersistentActor with ActorLogging {
    import DomainModel._

    val coupons: mutable.Map[String, User] = new mutable.HashMap[String, User]()

    override def persistenceId: String = "coupon-manager"

    override def receiveCommand: Receive = {
      case ApplyCoupon(coupon, user) =>
        if (!coupons.contains(coupon.code)) {
          persist(CouponApplied(coupon.code, user)) { e =>
            log.info(s"Persisted $e")
            coupons.put(coupon.code, user)
          }
        }
    }

    override def receiveRecover: Receive = {
      case event @ CouponApplied(code, user) => {
        log.info(s"Recovered: $event")
        coupons.put(code, user)
      }
    }
  }

  import DomainModel._

  val system = ActorSystem("DetachingModels", ConfigFactory.load().getConfig("detachingModels"))
  val couponManager = system.actorOf(Props[CouponManager], "couponManager")

  for (i <- 1 to 5) {
    val coupon = Coupon(s"MEGA_COUPON_$i", 100)
    val user = User(s"$i", s"user_$i@rtjvm.com")
    couponManager ! ApplyCoupon(coupon, user)
  }

}
