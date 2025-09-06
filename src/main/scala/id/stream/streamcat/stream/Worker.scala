package id.stream.streamcat.stream

import cats.*
import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import fs2.*
import org.typelevel.log4cats.Logger

class Worker[F[_]: Concurrent: Console, A] (
  name: String,
  q: Queue[F, Option[A]],
  op: A => F[Unit],
  logger: Logger[F]
) {

  def publish(job: A): F[Boolean] = q.tryOffer(Some(job))

  def stopWork = q.offer(None)

  def getName = name

  def work: F[Unit] =
    Stream.fromQueueNoneTerminated(q)
          .evalMap { job => 
            for
              _ <- logger.info(s"$name worker is taking job")
              _ <- op(job)
              _ <- logger.info(s"$name worker has finished the job")
            yield ()
          }
          .compile
          .drain

}
