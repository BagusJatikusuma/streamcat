package id.stream.streamcat.stream

import cats.*
import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.*

class Worker[F[_]: Concurrent, A] (
  name: String,
  workQueue: Queue[F, Option[A]],
  works: Ref[F, Vector[A]],
  op: A => F[Unit],
  logger: Logger[F]
) {

  def publish(job: A): F[Boolean] = 
    for
      res <- workQueue.tryOffer(Some(job))
      _   <- 
        if res then
          works.update(vec => vec.appended(job))
        else
          ().pure[F]
    yield res

  def stopWork = 
    workQueue.offer(None)

  def work: F[Unit] =
    Stream.fromQueueNoneTerminated(workQueue)
          .evalMap { job => 
            for
              _ <- logger.info(s"$name worker is taking job")
              _ <- op(job)
              _ <- works.update(vec => vec.drop(1))
              _ <- logger.info(s"$name worker has finished the job")
            yield ()
          }
          .compile
          .drain

  def getName = name

  def peekWorks: F[Vector[A]] = works.get

}

object Worker:

  def make[F[_]: Concurrent: Temporal, A](
    name: String,
    logger: Logger[F]
  ): F[Worker[F, WorkerJob]] =
    val op: WorkerJob => F[Unit] = job => Temporal[F].sleep(10.seconds)

    for
      queue  <- Queue.bounded[F, Option[WorkerJob]](5)
      works  <- Ref.of[F, Vector[WorkerJob]](Vector())
    yield Worker(name, queue, works, op, logger)
