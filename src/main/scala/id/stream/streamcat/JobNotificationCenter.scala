package id.stream.streamcat

import cats.effect.*
import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import fs2.*
import fs2.concurrent.*

import id.stream.streamcat.JobNotificationCenter.Notification

class JobNotificationCenter[F[_]: Async](topic: Topic[F, String]) {

  def publish(notif: Notification): F[Unit] = 
    topic.publish1(notif.asJson.noSpaces).void

  def subscribe: Stream[F, String] =
    topic.subscribe(1_000)
  
}

object JobNotificationCenter:
  
  case class Notification(message: String, payload: NotificationType) derives Encoder
  
  enum NotificationType:
    case WorkerAdded(name: String)
    case WorkerFired(name: String)
    case WorkTaken(workerName: String, url: String)
    case WorkDone(workerName: String, url: String)

  object NotificationType:
    given Encoder[NotificationType] with
      def apply(t: NotificationType): Json = t match
        case WorkerAdded(name) => Json.obj("type" -> "WorkerAdded".asJson, "payload" -> Json.obj("name" -> name.asJson))
        case WorkerFired(name) => Json.obj("type" -> "WorkerFired".asJson, "payload" -> Json.obj("name" -> name.asJson))
        case WorkTaken(name, url) => Json.obj("type" -> "WorkTaken".asJson, "payload" -> Json.obj("name" -> name.asJson, "url" -> url.asJson))
        case WorkDone(name, url)  => Json.obj("type" -> "WorkDone".asJson, "payload" -> Json.obj("name" -> name.asJson, "url" -> url.asJson))

  def make[F[_]: Async]: F[JobNotificationCenter[F]] =
    for
      t   <- Topic[F, String]
    yield JobNotificationCenter(t)
  
