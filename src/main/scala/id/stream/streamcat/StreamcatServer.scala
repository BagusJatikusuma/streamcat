package id.stream.streamcat

import cats.effect.Async
import cats.effect.std.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.io.net.Network
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*

import id.stream.streamcat.stream.Event

object StreamcatServer:

  def run[F[_]: Async: Network: Console](queue: Queue[F, Event]): F[Nothing] = {
    for {
      client <- EmberClientBuilder.default[F].build
      helloWorldAlg = HelloWorld.impl[F]
      jokeAlg = Jokes.impl[F](client)

      jobRoutes = JobRoutes[F](queue)

      // Combine Service Routes into an HttpApp.
      // Can also be done via a Router if you
      // want to extract segments not checked
      // in the underlying routes.
      httpApp = (
        StreamcatRoutes.helloWorldRoutes[F](helloWorldAlg) <+>
        StreamcatRoutes.jokeRoutes[F](jokeAlg) <+>
        jobRoutes.publicRoutes
      ).orNotFound

      // With Middlewares in place
      finalHttpApp = httpApp

      _ <- 
        EmberServerBuilder.default[F]
          .withHost(ipv4"0.0.0.0")
          .withPort(port"8080")
          .withHttpApp(finalHttpApp)
          .build
    } yield ()
  }.useForever
