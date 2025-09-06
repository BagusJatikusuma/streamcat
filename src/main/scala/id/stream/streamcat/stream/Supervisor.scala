package id.stream.streamcat.stream

import cats.effect.*
import cats.effect.std.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.*
import org.typelevel.log4cats.Logger
import scala.concurrent.duration.*

import id.stream.streamcat.stream.Command.*
import id.stream.streamcat.stream.Supervisor.DistributorQueue

case class WorkerJob(id: String)

class Supervisor[F[_]: Async: Console: Temporal] private (
  name: String,
  q: Queue[F, Event],
  workers: Ref[F, List[Worker[F, WorkerJob]]],
  logger: Logger[F],
  distributor: DistributorQueue[F]
) {

  def run: Stream[F, Unit] =
    Stream
      .fromQueueUnterminated(q)
      .evalMap { _.cmd match
        
        case InitWorker(workername) =>
          val op: WorkerJob => F[Unit] = workerJob => 
            for
              _   <- Temporal[F].sleep(5.seconds)
            yield ()

          for
            _      <- logger.info(s"supervisor $name initing workers")
            queue  <- Queue.bounded[F, Option[WorkerJob]](1)
            worker =  Worker(workername, queue, op, logger)
            _      <- worker.work.start
            _      <- distributor.addWorker(worker)
            _      <- workers.update(lst => worker :: lst)
          yield ()

        case FireWorker(workername) => 
          for
            lst <- workers.get
            _   <- lst.find(worker => worker.getName == workername) match
              case None => ().pure[F]
              case Some(worker) => 
                for 
                  _ <- worker.stopWork
                  _ <- workers.update(lst => lst.filterNot(_.getName == workername))
                  _ <- distributor.removeWorker(workername)
                yield ()
            
          yield ()

        case MakeWork => 
          val job = WorkerJob(id = java.util.UUID.randomUUID().toString())
          delegateWork(job).void
      
      }

  private def delegateWork(job: WorkerJob): F[Boolean] = 
    distributor.next.flatMap {
      //no worker available
      case None => 
        false.pure[F]

      case Some(worker) =>
        worker.publish(job).flatMap {
          case true   => true.pure[F]
          case false  => delegateWork(job) 
        }
    }
    // workers.get.flatMap { lst =>
    //   def loop(currList: List[Worker[F, WorkerJob]]): F[Boolean] = currList match
    //     case Nil => false.pure[F]

    //     case worker :: rest =>
    //       worker.publish(job).flatMap {
    //         case true   => true.pure[F]
    //         case false  => loop(rest)
    //       }
       
    //   loop(lst)
    // }
  
}

object Supervisor:

  def make[F[_]: Async: Console: Temporal](
    name: String, 
    q: Queue[F, Event],
    logger: Logger[F]
  ): F[Supervisor[F]] =
    for
      workers <- Ref.of(List[Worker[F, WorkerJob]]())
      workDistributor <- DistributorQueue.make[F]
    yield Supervisor(name, q, workers, logger, workDistributor)

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