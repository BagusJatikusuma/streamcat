package id.stream.streamcat

import cats.effect.{IO, IOApp}
import cats.effect.std.Queue
import fs2.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

import id.stream.streamcat.stream.Event
import id.stream.streamcat.stream.Supervisor

object Main extends IOApp.Simple:
  val run =
    for
      logger      <- Slf4jLogger.create[IO]
      q           <- Queue.unbounded[IO, Event]
      supervisor  <- Supervisor.make("super-root", q, logger)

      _ <- Stream
                .eval(StreamcatServer.run[IO](q, logger)).concurrently(supervisor.run)
                .compile.drain

    yield ()
