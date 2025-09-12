package id.stream.streamcat.stream

import cats.effect.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.*
import org.typelevel.log4cats.Logger

import id.stream.streamcat.stream.Command.*
import id.stream.streamcat.stream.Supervisor.DistributorQueue

// import scala.concurrent.duration.*

import id.stream.streamcat.JobNotificationCenter
import id.stream.streamcat.JobNotificationCenter.Notification
import id.stream.streamcat.JobNotificationCenter.NotificationType

case class WorkerJob(id: String)

class Supervisor[F[_]: Async: Console: Temporal: Random] private (
  name: String,
  q: Queue[F, Event],
  notifCenter: JobNotificationCenter[F],
  workers: Ref[F, List[Worker[F, WorkerJob]]],
  logger: Logger[F],
  distributor: DistributorQueue[F]
) {

  def getWorkers: F[List[Worker[F, WorkerJob]]] =
    workers.get

  def run: Stream[F, Unit] =
    // val checkWorkersJobs =
    //   Stream.awakeEvery(10.seconds)
    //         .evalMap {_ =>
    //           for
    //             ws <- workers.get
    //             _  <- ws.traverse { worker => 
    //               for
    //                 jobs <- worker.peekWorks
    //                 _    <- logger.info(s"check worker =>  worker: ${worker.getName} has ${jobs.length}")
    //               yield ()
    //             }
    //           yield ()  
    //         }

    val mainOp = Stream
      .fromQueueUnterminated(q)
      .evalMap { _.cmd match
        
        case InitWorker(workername) =>
          for
            _      <- logger.info(s"supervisor $name initing workers")
            worker <- Worker.make(workername, logger, notifCenter)
            _      <- worker.work.start
            _      <- distributor.addWorker(worker)
            _      <- workers.update(lst => worker :: lst)

            notif  =  Notification(message = s"worker $workername is added", payload = NotificationType.WorkerAdded(workername))
            _      <- notifCenter.publish(notif)
          yield ()

        case FireWorker(workername) => 
          for
            lst <- workers.get
            _   <- lst.find(worker => worker.getName == workername) match
              case None => ().pure[F]
              case Some(worker) => 
                for 
                  _   <- worker.stopWork
                  _   <- workers.update(lst => lst.filterNot(_.getName == workername))
                  _   <- distributor.removeWorker(workername)
                  msg =  Notification(message = s"Worker $workername removed", payload = NotificationType.WorkerFired(workername))
                  _   <- notifCenter.publish(msg)
                yield ()
            
          yield ()

        case MakeWork => 
          val job = WorkerJob(id = java.util.UUID.randomUUID().toString())
          for
            optWorker <- delegateWork(job)
            _         <- optWorker match 
              case Some(worker) => 
                val msg = Notification(message = s"Job has added to $worker", payload = NotificationType.WorkTaken(worker))
                notifCenter.publish(msg)
              case None => ().pure[F]
              
          yield ()
      
      }
    
    mainOp

  private def delegateWork(job: WorkerJob): F[Option[String]] = 
    distributor.next.flatMap {
      //no worker available
      case None => 
        None.pure[F]

      case Some(worker) =>
        worker.delegate(job).flatMap {
          case opt@Some(_) => opt.pure[F] 
          case None        => delegateWork(job) 
        }
    }
  
}

object Supervisor:

  def make[F[_]: Async: Console: Temporal: Random](
    name: String, 
    q: Queue[F, Event],
    notifCenter: JobNotificationCenter[F],
    logger: Logger[F]
  ): F[Supervisor[F]] =
    for
      workers         <- Ref.of(List[Worker[F, WorkerJob]]())
      workDistributor <- DistributorQueue.make[F]
    yield Supervisor(name, q, notifCenter, workers, logger, workDistributor)

  private class DistributorQueue[F[_]: Concurrent](
    workerQueue: Queue[F, Worker[F, WorkerJob]],
    removedRef: Ref[F, Vector[String]]
  ) {

    def addWorker(worker: Worker[F, WorkerJob]): F[Unit] =
      workerQueue.offer(worker)

    def removeWorker(workerName: String): F[Unit] =
      removedRef.update(vec => vec.appended(workerName))

    def next: F[Option[Worker[F, WorkerJob]]] = 
      workerQueue.tryTake.flatMap {
        case None => 
          None.pure[F]

        case Some(worker) => 
          for
            removeds  <- removedRef.get
            //check if intended worker is in removed list
            workerOpt <- removeds.find(wn => wn == worker.getName) match
              //if not in removed list then set it in last item in queue
              case None => 
                workerQueue.offer(worker) *> Some(worker).pure[F]
              //if intended worker in removed list then 
              //first remove it from removed list 
              //then get next worker from queue
              case Some(removedName) =>
                for
                  _   <- removedRef.update(v => v.filterNot(wn => wn == removedName))
                  opt <- next
                yield opt

          yield workerOpt
      }

  }

  private object DistributorQueue:

    def make[F[_]: Concurrent]: F[DistributorQueue[F]] =
      for
        q   <- Queue.unbounded[F, Worker[F, WorkerJob]]
        ref <- Ref.of(Vector[String]())
      yield DistributorQueue(q, ref)