package id.stream.streamcat

import cats.effect.{IO, IOApp}
import cats.effect.std.Queue
import io.circe.*
import io.circe.syntax.*
import fs2.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

import id.stream.streamcat.stream.Event
import id.stream.streamcat.stream.Supervisor

case class TopicMessage(message: String, payload: TopicType) derives Encoder

enum TopicType:
  case WorkerAdded(name: String)
  case WorkerFired(name: String)
  case WorkTaken
  case WorkDone

object TopicType:
  given Encoder[TopicType] with
    def apply(t: TopicType): Json = t match
      case WorkerAdded(name) => Json.obj("type" -> "WorkerAdded".asJson, "payload" -> Json.obj("name" -> name.asJson))
      case WorkerFired(name) => Json.obj("type" -> "WorkerFired".asJson, "payload" -> Json.obj("name" -> name.asJson))
      case WorkTaken         => Json.obj("type" -> "WorkTaken".asJson)
      case WorkDone          => Json.obj("type" -> "WorkDone".asJson)

object Main extends IOApp.Simple:
  val run =
    for
      logger      <- Slf4jLogger.create[IO]
      q           <- Queue.unbounded[IO, Event]
      notifCenter <- JobNotificationCenter.make[IO]
      supervisor  <- Supervisor.make[IO]("super-root", q, notifCenter, logger)

      _ <- Stream
                .eval(StreamcatServer.run[IO](q, notifCenter, logger, supervisor)).concurrently(supervisor.run)
                .compile.drain

    yield ()


