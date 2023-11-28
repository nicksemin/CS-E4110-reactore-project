// group 17
// 123456 Firstname Lastname
// 936323 Nikolai Semin

package reactor

import reactor.api.{Event, EventHandler}
import scala.collection.mutable.Set

final class Dispatcher(private val queueLength: Int = 10) {
  require(queueLength > 0)

  private val eventsQueue = new BlockingEventQueue[Event[T]](queueLength)
  private val handlers: Set[EventHandler[T]] = Set()


  /* The idea would be to start a new thread for each added handler monitoring events in there.
  Once a handle is removed the corresponding thread should be interrupted.


  */
  @throws[InterruptedException]
  def handleEvents(): Unit = {
    while(!handlers.isEmpty) {
        handlers.foreach { handler =>
           val workerThread = new Thread {
            override def run {
                val handle = handler.getHandle
                var event = handle.read
                while (event != null) {
                    eventsQueue.enqueue(Event(event, handler))
                    event = handle.read
                }
                // cancel the thread after getting null
            }
           }
        }
    // woprth checking if a thread for a handler (still) running and launch a new one if needed
    // then pop an event from a queue and handle it, go through all events 

   }  
  }

  def addHandler[T](h: EventHandler[T]): Unit = {
    handlers += h
  }

  def removeHandler[T](h: EventHandler[T]): Unit = {
    handlers -= h
    // cancel thread assoicated with the handler 
  }

//   // Hint:
//   final class WorkerThread[T]() extends Thread {   

//    override def run(): Unit = {
//     handlers.foreach { handler =>
//     //    handler.get
//    }   

//    def cancelThread(): Unit = ???  

//   }
//   }

}

