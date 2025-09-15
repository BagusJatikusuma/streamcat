package id.stream.streamcat.stream

import cats.*
import cats.effect.*
import cats.effect.std.*
import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import fs2.*
import org.typelevel.log4cats.Logger

import id.stream.streamcat.JobNotificationCenter
import id.stream.streamcat.JobNotificationCenter.Notification
import id.stream.streamcat.JobNotificationCenter.NotificationType

class Worker[F[_]: Concurrent] (
  name: String,
  workQueue: Queue[F, Option[WorkerJob]],
  works: Ref[F, Vector[WorkerJob]],
  op: WorkerJob => F[Unit],
  logger: Logger[F],
  notifCenter: JobNotificationCenter[F]
) {

  def delegate(job: WorkerJob): F[Option[String]] = 
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
              
              jobTakenMsg = Notification(message = s"Worker $name start scraping ${job.url}", payload = NotificationType.WorkTaken(name, job.url))
              _   <- notifCenter.publish(jobTakenMsg) 

              _   <- op(job)
              _   <- works.update(vec => vec.drop(1))
            yield ()
          }
          .compile
          .drain

  def getName = name

  def peekWorks: F[Vector[WorkerJob]] = works.get

}

object Worker:

  def make[F[_]: Async: Temporal: Random](
    name: String,
    logger: Logger[F],
    notifCenter: JobNotificationCenter[F]
  ): F[Worker[F]] =
    val op: WorkerJob => F[Unit] = job =>
      Scraper.make[F].use { scraper => 
        for 
          title <- scraper.getTitle(job.url)
          msg   =  Notification(message = s"finish get title of ${job.url} done by $name and the title is $title", payload = NotificationType.WorkDone(name, job.url))
          _     <- notifCenter.publish(msg)
        yield ()
      }

    for
      queue  <- Queue.bounded[F, Option[WorkerJob]](5)
      works  <- Ref.of[F, Vector[WorkerJob]](Vector())
    yield Worker(name, queue, works, op, logger, notifCenter)

  given [F[_]: Concurrent: Temporal]: Encoder[Worker[F]] with
    def apply(worker: Worker[F]): Json = 
      Json.obj("workerName" -> worker.getName.asJson)

  import org.http4s.*
  import org.http4s.circe.*

  given [F[_]: Concurrent: Temporal]: EntityEncoder[F, Worker[F]] = jsonEncoderOf
  given [F[_]: Concurrent: Temporal]: EntityEncoder[F, List[Worker[F]]] = jsonEncoderOf
