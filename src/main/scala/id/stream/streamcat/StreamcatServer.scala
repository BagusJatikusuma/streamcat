package id.stream.streamcat

import cats.effect.Async
import cats.effect.std.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.io.net.Network
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.log4cats.Logger
import fs2.io.file.Files

import id.stream.streamcat.stream.Event

object StreamcatServer:

  def run[F[_]: Async: Files: Network](
    queue: Queue[F, Event],
    notifCenter: JobNotificationCenter[F],
    logger: Logger[F],
    supervisor: id.stream.streamcat.stream.Supervisor[F]
  ): F[Unit] =
    for {
      _   <- logger.info("starting server")

      jobRoutes = JobRoutes[F](queue, notifCenter, supervisor)

      _ <- 
        EmberServerBuilder.default[F]
          .withHost(ipv4"0.0.0.0")
          .withPort(port"8080")
          .withHttpWebSocketApp(wsb => (jobRoutes.publicRoutes <+> jobRoutes.wsRoutes(wsb)).orNotFound)
          .build
          .useForever

    } yield ()
