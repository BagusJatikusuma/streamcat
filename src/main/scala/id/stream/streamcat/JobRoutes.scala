package id.stream.streamcat

import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.circe.*
import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.websocket.*
import org.http4s.server.websocket.WebSocketBuilder2
import fs2.*
import fs2.io.file.Files

import id.stream.streamcat.stream.Event
import id.stream.streamcat.stream.Command

import org.http4s.circe.*

final case class AddWorker(name: String) derives Decoder
object AddWorker:
  given [F[_]: Concurrent]: EntityDecoder[F, AddWorker] = jsonOf

final case class AddJob(url: String) derives Decoder
object AddJob:
  given [F[_]: Concurrent]: EntityDecoder[F, AddJob] = jsonOf

class JobRoutes[F[_]: Files: Async](
  queue: Queue[F, Event],
  notifCenter: JobNotificationCenter[F],
  supervisor: id.stream.streamcat.stream.Supervisor[F]
) extends Http4sDsl[F] {

  val workDir = "/home/zenbook/GitLocalRepository/scala3/streamcat"

  def publicRoutes = HttpRoutes.of[F] {

    case req @ GET -> Root =>
      StaticFile
        .fromPath(
          fs2.io.file.Path(s"$workDir/public/chat.html")
        )
        .getOrElseF(NotFound())

    case GET -> Root / "worker" =>
      for
        workers <- supervisor.getWorkers
        resp    <- Ok(workers)
      yield resp

    case req @ POST -> Root / "worker" => 
      for
        payload <- req.as[AddWorker]
        _       <- queue.offer(Event(id = java.util.UUID.randomUUID().toString, cmd = Command.InitWorker(payload.name)))
        resp    <- Ok(s"worker added")
      yield resp

    case req @ POST -> Root / "make-work" =>
      for 
        job   <- req.as[AddJob]
        _     <- queue.offer(Event(id = java.util.UUID.randomUUID().toString, cmd = Command.MakeWork(job.url)))
        resp  <- Ok(s"Work delegated")
      yield resp
  
  }

  def wsRoutes(wsb: WebSocketBuilder2[F]) = HttpRoutes.of[F] {

    case GET -> Root / "ws" =>
      val send: Stream[F, WebSocketFrame] = notifCenter.subscribe.map(str => WebSocketFrame.Text(str))

      val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(msg => Async[F].delay(println(msg)))

      wsb.build(send, receive)

  }
  
}
