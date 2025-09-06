package id.stream.streamcat

import cats.effect.Async
import cats.effect.std.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.io.net.Network
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.log4cats.Logger

import id.stream.streamcat.stream.Event

object StreamcatServer:

  def run[F[_]: Async: Network](
    queue: Queue[F, Event],
    logger: Logger[F]
  ): F[Unit] =
    for {
      _   <- logger.info("starting server")

      jobRoutes = JobRoutes[F](queue)

      httpApp = jobRoutes.publicRoutes.orNotFound

      // With Middlewares in place
      finalHttpApp = httpApp

      _ <- 
        EmberServerBuilder.default[F]
          .withHost(ipv4"0.0.0.0")
          .withPort(port"8080")
          .withHttpApp(finalHttpApp)
          .build
          .useForever

    } yield ()
