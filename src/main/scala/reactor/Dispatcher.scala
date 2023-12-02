// group 17
// 906984 Yerzhan Zhamashev
// 936323 Nikolai Semin

package reactor

import reactor.api.{Event, EventHandler, Handle}
import scala.collection.mutable.{Set}
import scala.collection.immutable.{List}

/**
 * Dispatcher class is responsible for managing event handlers and dispatching events to them.
 * 
 * @param queueLength The maximum length of the event queue. Must be greater than 0.
 */
final class Dispatcher(private val queueLength: Int = 10) {
  require(queueLength > 0, "Queue length must be greater than 0.")

  private val handlers: Set[EventHandler[_]] = Set()
  private val eventsQueue = new BlockingEventQueue[Any](queueLength)
  private var threadList: List[WorkerThread[_]] = List.empty

  /**
   * Continuously handles events from the queue as long as there are handlers available.
   * 
   * @throws InterruptedException if the thread is interrupted.
   */
  @throws[InterruptedException]
  def handleEvents(): Unit = {
    while (handlers.nonEmpty) {
      val event = eventsQueue.dequeue
      if (handlers.contains(event.getHandler)) {
        event.dispatch() // Dispatch the event if its handler is present.
      }
    }
  }

  /**
   * Adds an event handler for the dispatcher to manage.
   * 
   * @param h The event handler to add.
   * @tparam T The type of event data the handler is dealing with.
   */
  def addHandler[T](h: EventHandler[T]): Unit = {
    if (handlers.add(h)) {
      addAndStartHandlerWorker(h)
    }
  }

  /**
   * Starts a new worker thread for an event handler.
   * 
   * @param h The event handler to add.
   * @tparam T The type of event data the handler is dealing with.
   */
  private def addAndStartHandlerWorker[T](h: EventHandler[T]): Unit = {
    val worker = new WorkerThread(h)
    threadList = worker :: threadList
    worker.start()
  }

  /**
   * Removes an event handler and stops its associated worker thread.
   * 
   * @param h The event handler to remove.
   * @tparam T The type of event data the handler is dealing with.
   */
  def removeHandler[T](h: EventHandler[T]): Unit = {
    if (handlers.remove(h)) {
      // Stop and remove the threads associated with the handler from the list
      threadList = threadList.filter(thread => {
        if (thread.getHandler == h) {
          thread.cancelThread()
          false
        } else {
          true
        }
      })
    }
  }

  /**
   * Inner class representing a worker thread for each event handler.
   */
  private final class WorkerThread[T](private val handler: EventHandler[T]) extends Thread {
    def getHandler: EventHandler[_] = handler

    override def run(): Unit = {
      try {
        // Get the handle associated with the event handler a read the first event data
        var eventData = handler.getHandle.read
        while (eventData != null && !this.isInterrupted) {
          eventsQueue.enqueue(Event(eventData, handler)) // Enqueue the event to dispatch it in correct order.
          eventData = handler.getHandle.read // Continue reading event data.
        }

        // Enqueue the null event to signal the end of the event stream.
        if (!this.isInterrupted) 
          eventsQueue.enqueue(Event(eventData, handler))
      } catch {
        case e: InterruptedException => {
          // Do nothing. The thread cannot work anymore (if eventsQueue.enqueue throws InterruptedException.)
        }
      }
    }

    def cancelThread(): Unit = {
      this.interrupt()
    }
  }
}
