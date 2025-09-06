package id.stream.streamcat

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.circe.*
import org.http4s.*
import org.http4s.dsl.Http4sDsl

import id.stream.streamcat.stream.Event
import id.stream.streamcat.stream.Command

import org.http4s.circe.*

final case class AddWorker(name: String) derives Decoder
object AddWorker:
  given [F[_]: Concurrent]: EntityDecoder[F, AddWorker] = jsonOf

class JobRoutes[F[_]: Concurrent: Console](
  queue: Queue[F, Event]
) extends Http4sDsl[F] {

  def publicRoutes = HttpRoutes.of[F] {

    case req @ POST -> Root / "worker" => 
      for
        payload <- req.as[AddWorker]
        _       <- queue.offer(Event(id = java.util.UUID.randomUUID().toString, cmd = Command.InitWorker(payload.name)))
        resp    <- Ok(s"worker added")
      yield resp

    case GET -> Root / "make-work" =>
      for 
        _     <- queue.offer(Event(id = java.util.UUID.randomUUID().toString, cmd = Command.MakeWork))
        resp  <- Ok(s"Work delegated")
      yield resp
  
  }
  
}
