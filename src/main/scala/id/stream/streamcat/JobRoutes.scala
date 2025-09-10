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
import fs2.concurrent.Topic

final case class AddWorker(name: String) derives Decoder
object AddWorker:
  given [F[_]: Concurrent]: EntityDecoder[F, AddWorker] = jsonOf

class JobRoutes[F[_]: Async](
  queue: Queue[F, Event],
  topic: Topic[F, String]
) extends Http4sDsl[F] {

  given Files[F] = Files.forAsync

  def publicRoutes = HttpRoutes.of[F] {

    case req @ GET -> Root =>
      StaticFile
        .fromPath(
          fs2.io.file.Path(getClass.getClassLoader.getResource("chat.html").getFile())
        )
        .getOrElseF(NotFound())

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

  def wsRoutes(wsb: WebSocketBuilder2[F]) = HttpRoutes.of[F] {

    case GET -> Root / "ws" =>
      val send: Stream[F, WebSocketFrame] = topic.subscribe(1000).map(str => WebSocketFrame.Text(str))

      val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(msg => Async[F].delay(println(msg)))

      wsb.build(send, receive)

  }
  
}
