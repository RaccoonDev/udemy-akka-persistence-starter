package part4_practices

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.{Offset, PersistenceQuery}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.util.Random

object PersistenceQueryDemo extends App {

  implicit val system = ActorSystem("PersistenceQueryDemo", ConfigFactory.load().getConfig("persistenceQuery"))

  // read journal
  val readJournal = PersistenceQuery(system)
    .readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  val persistenceIds = readJournal.persistenceIds()
  implicit val materializer = ActorMaterializer()
  persistenceIds.runForeach(id =>
    println(s"Found persistence ID: $id")
  )

  class SimplePersistenceActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "persistence-query-id-1"

    override def receiveCommand: Receive = {
      case m => persist(m) { e =>
        log.info(s"Persisted: $e")
      }
    }

    override def receiveRecover: Receive = {
      case e => log.info(s"Recovered: $e")
    }
  }

  val simpleActor = system.actorOf(Props[SimplePersistenceActor], "simplePersistentActor")

  import system.dispatcher

  system.scheduler.scheduleOnce(5.seconds) {
    simpleActor ! "hello"
  }

  // events by persistence id
  val events = readJournal.eventsByPersistenceId("coupon-manager", 0, Long.MaxValue)
  events.runForeach(println)

  // events by tags
  val genres = Array("pop", "rock", "jazz", "folk", "disco")
  case class Song(artist: String, title: String, genre: String)
  // command
  case class Playlist(songs: List[Song])
  // event
  case class PlaylistPurchased(id: Int, songs: List[Song])

  class MusicStoreCheckoutActor extends PersistentActor with ActorLogging {
    override def persistenceId: String = "music-store-checkout"

    var latestPlaylistId = 0

    override def receiveCommand: Receive = {
      case Playlist(songs) =>
        persist(PlaylistPurchased(latestPlaylistId, songs)) { _ =>
          log.info(s"User purchased: $songs")
          latestPlaylistId += 1
        }
    }

    override def receiveRecover: Receive = {
      case event @ PlaylistPurchased(id, _) =>
        log.info(s"Recovered: $event")
        latestPlaylistId = id
    }
  }

  class MusicStoreEventAdapter extends WriteEventAdapter {
    override def manifest(event: Any): String = "music-store-event-adapter"

    override def toJournal(event: Any): Any = event match {
      case event @ PlaylistPurchased(_, songs) =>
        Tagged(event, songs.map(_.genre).toSet)
      case event => event
    }
  }

  val checkoutActor = system.actorOf(Props[MusicStoreCheckoutActor], "musicStoreActor")

  val r = new Random()
  for(_ <- 1 to 10) {
    val maxSongs = r.nextInt(5)
    val songs = for (i <- 1 to maxSongs) yield {
      val randomGenre = genres(r.nextInt(genres.size))
      Song(s"Artist $i", s"My Love Song $i", randomGenre)
    }
    checkoutActor ! Playlist(songs.toList)
  }

  val rockPlaylists = readJournal.eventsByTag("rock", Offset.noOffset)
  rockPlaylists.runForeach { e =>
    println(s"Found a playlist with a rock songs: $e")
  }
}