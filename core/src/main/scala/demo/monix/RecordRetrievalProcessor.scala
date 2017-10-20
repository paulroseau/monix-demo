package demo.monix

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.Random

import monix.eval.{ Task }
import monix.execution.{ Ack, Cancelable }
import monix.reactive.{ Observable, Observer }

import monix.execution.Scheduler

object RecordRetrievalProcessor {
  type Error = String
  type Inquiry = Int
  type Result = String

  implicit val nonDaemonicThreadScheduler: Scheduler = 
    Scheduler.forkJoin(
      name = "my-forkjoin", 
      parallelism = 4, 
      maxThreads = 128, 
      daemonic = false
    )

  def main(args: Array[String]): Unit = {
    val processor = new RecordRetrievalProcessor
    val stopProcessor = new Runnable {
      def run(): Unit = {
        processor.stop()
      }
    }
    processor.start()
    nonDaemonicThreadScheduler.scheduleOnce(15, TimeUnit.SECONDS, stopProcessor)
    sys.addShutdownHook {
      println("app was killed, bye bye")
    }
    ()
  }

  def getLastlyInserted(): Task[Either[Error, Seq[Inquiry]]] =
    Task.fork(
      Task.delay { 
        println(s"Fetching inquiries... ")
        Thread.sleep(3000)
        if (Random.nextInt(3) % 3 != 0) {
          Right(Seq(1, 2, 3, 4, 5))
        } else {
          Left("error")
        }
      }
    )

  def getByInquiryId(inquiry: Inquiry): Task[Either[Error, Option[Result]]] =
    Task.fork(
      Task.delay { 
        println(s"Fetching records for inquiry $inquiry...")
        Thread.sleep(Random.nextInt(3) * 1000 + 1000)
        if (inquiry == 2) {
          val error = s"Error for inquiry $inquiry"
          println(error)
          Left(error) 
        }
        else if (inquiry == 4) {
          val record = s"Some records saved for inquiry $inquiry"
          println(record)
          Right(Some(record))
        }
        else {
          println(s"No record found for inquiry $inquiry")
          Right(None)
        }
      }
    )

  def fetchAndInsertRecords(inquiry: Inquiry): Task[Either[Error, Result]] =
    Task.fork {
      println(s"Fetching and inserting new records for inquiry $inquiry...")
      if (inquiry == 1) {
        println(s"Unexpected error while fetching record for inquiry $inquiry")
        Task.raiseError[Either[String, Result]](new IllegalArgumentException(s"inquiry $inquiry"))
      }
      else {
        Task.delay {
          Thread.sleep(Random.nextInt(3) * 1000 + 1000)
          println(s"Found record for inquiry $inquiry")
          Right(s"Record for $inquiry")
        }
      }
    }
}

class RecordRetrievalProcessor {
  import RecordRetrievalProcessor._

  private var currentTask: Cancelable = null

  def start(): Unit = {
    val s = source
    val o = observer
    currentTask = s.subscribe(o)
  }

  def stop(): Unit = {
    if (currentTask != null) {
      println(s"Stopping processor")
      currentTask.cancel
    }
  }

  def source: Observable[Seq[Inquiry]] = {
    Observable
      .intervalAtFixedRate(1.seconds)
      .flatMap { _ ⇒
        println("Tick, let go for a round !")
        Observable.fromTask(getLastlyInserted())
      }
      .flatMap {
        case Left(error) => {
          println("could not fetch inquiries :-(")
          Observable.empty[Seq[Inquiry]]
        }
        case Right(inquiries) => {
          println(s"got inquiries : $inquiries")
          Observable.fromTask(
            Task
              .gatherUnordered(
                inquiries.map { inquiry ⇒
                  getByInquiryId(inquiry)
                    .map(resultOptOrError ⇒ resultOptOrError.map(resultOpt ⇒ inquiry -> resultOpt))
                }
              )
              .map { inquiryEithers ⇒
                inquiryEithers.collect { case Right((inquiry, None)) ⇒ inquiry } // TODO : we ignore errors silently here
              }
          )
        }
      }
  }

  def observer: Observer[Seq[Inquiry]] = {
    new Observer[Seq[Inquiry]] {
      def onNext(inquiries: Seq[Inquiry]): Future[Ack] = {
        val task =
          Task.gatherUnordered {
            inquiries.map { inquiry ⇒
              fetchAndInsertRecords(inquiry)
                .onErrorRecoverWith {
                  case NonFatal(e) ⇒ {
                    println(s"An unexpected error occured... wrapping it")
                    Task.now(Left("boom"))
                  }
                }
            }
          }
            .map(_ ⇒ Ack.Continue)

        task.runAsync
      }

      def onError(ex: Throwable): Unit = {
        println("Could not retrieve records for pending inquiries")
        println(ex)
      }

      def onComplete(): Unit = { }
    }
  }
}
