package id.stream.streamcat

import cats.effect.{IO, IOApp}
import cats.effect.std.Queue
import fs2.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

import id.stream.streamcat.stream.Event
import id.stream.streamcat.stream.Supervisor

object Main extends IOApp.Simple:
  val run = 
    val logger = Slf4jLogger.getLogger[IO]

    for
      q           <- Queue.unbounded[IO, Event]
      supervisor  <- Supervisor.make("super-root", q, logger)

      _ <- Stream
                .eval(StreamcatServer.run[IO](q)).concurrently(supervisor.run)
                .compile.drain

    yield ()
