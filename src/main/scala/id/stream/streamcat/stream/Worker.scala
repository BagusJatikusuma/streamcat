package id.stream.streamcat.stream

import cats.*
import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import fs2.*
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.*
import id.stream.streamcat.JobNotificationCenter
import id.stream.streamcat.JobNotificationCenter.Notification
import id.stream.streamcat.JobNotificationCenter.NotificationType

class Worker[F[_]: Concurrent, A] (
  name: String,
  workQueue: Queue[F, Option[A]],
  works: Ref[F, Vector[A]],
  op: A => F[Unit],
  logger: Logger[F],
  notifCenter: JobNotificationCenter[F]
) {

  def delegate(job: A): F[Option[String]] = 
    for
      res <- workQueue.tryOffer(Some(job))
      _   <- 
        if res then
          works.update(vec => vec.appended(job))
        else
          ().pure[F]
    yield if res then Some(name) else None

  def stopWork = 
    workQueue.offer(None)

  def work: F[Unit] =
    Stream.fromQueueNoneTerminated(workQueue)
          .evalMap { job => 
            for
              _   <- logger.info(s"$name worker is taking job")
              
              jobTakenMsg = Notification(message = s"Worker $name start processing the job", payload = NotificationType.WorkTaken(name))
              _   <- notifCenter.publish(jobTakenMsg) 

              _   <- op(job)
              _   <- works.update(vec => vec.drop(1))
              _   <- logger.info(s"$name worker has finished the job")
              msg =  Notification(message = s"Worker done by $name", payload = NotificationType.WorkDone(name))
              _   <- notifCenter.publish(msg)
            yield ()
          }
          .compile
          .drain

  def getName = name

  def peekWorks: F[Vector[A]] = works.get

}

object Worker:

  def make[F[_]: Concurrent: Temporal: Random, A](
    name: String,
    logger: Logger[F],
    notifCenter: JobNotificationCenter[F]
  ): F[Worker[F, WorkerJob]] =
    val op: WorkerJob => F[Unit] = job => 
      for
        duration <- Random[F].betweenInt(5, 15)
        _        <- logger.info(s"worker $name will do the job for $duration seconds")
        _        <- Temporal[F].sleep(duration.seconds)
      yield ()

    for
      queue  <- Queue.bounded[F, Option[WorkerJob]](5)
      works  <- Ref.of[F, Vector[WorkerJob]](Vector())
    yield Worker(name, queue, works, op, logger, notifCenter)

  given [F[_]: Concurrent: Temporal]: Encoder[Worker[F, WorkerJob]] with
    def apply(worker: Worker[F, WorkerJob]): Json = 
      Json.obj("workerName" -> worker.getName.asJson)

  import org.http4s.*
  import org.http4s.circe.*

  given [F[_]: Concurrent: Temporal]: EntityEncoder[F, Worker[F, WorkerJob]] = jsonEncoderOf
  given [F[_]: Concurrent: Temporal]: EntityEncoder[F, List[Worker[F, WorkerJob]]] = jsonEncoderOf
