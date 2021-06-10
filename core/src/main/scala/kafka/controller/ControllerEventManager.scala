/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

/**
 * Controller事件处理器接口，当前Kafka只有一个默认实现类 {@link KafkaController}
 */
trait ControllerEventProcessor {
  /**
   * 普通处理：接收一个 {@link ControllerEvent} 事件，按顺序处理
   *
   * @param event
   */
  def process(event: ControllerEvent): Unit

  /**
   * 抢占式处理：接收一个 {@link ControllerEvent} 事件，优先处理
   * 目前源码中只有两类事件需要抢占式处理：①ShutdownEventThread;②Expire
   *
   * @param event
   */
  def preempt(event: ControllerEvent): Unit
}

/**
 * 表示阻塞队列中的事件对象
 *
 * @param event         Controller事件
 * @param enqueueTimeMs 入队时间
 */
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {

  // 标记事件是否开始被处理。
  // 这里使用「CountDownLatch」是确保Expire事件在建立Zookeeper会话前被处理
  val processingStarted = new CountDownLatch(1)

  // 标记事件是否被处理过
  val spent = new AtomicBoolean(false)

  /**
   * 事件普通处理
   *
   * @param processor 事件处理器
   */
  def process(processor: ControllerEventProcessor): Unit = {
    // #1 如果事件已经被处理，直接返回
    if (spent.getAndSet(true))
      return
    // #2 释放在这把锁，唤醒在这把锁等待的线程
    processingStarted.countDown()

    // #3 将事件交给事件处理器处理
    processor.process(event)
  }

  /**
   * 事件抢占式处理
   *
   * @param processor
   */
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
    // #1 如果事件已经被处理，直接返回
      if (spent.getAndSet(true)) {
        return
      }
    // #2 将事件交给事件处理器以抢占式处理
    processor.preempt(event)
  }

  /**
   * 阻塞等待事件被处理完成
   */
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

/**
 *
 * @param controllerId              Controller所在的Broker ID
 * @param processor                 Controller事件真正处理者
 * @param time                      时间工具类
 * @param rateAndTimeMetrics        监控指标
 * @param eventQueueTimeTimeoutMs   正时时间
 */
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {

  import ControllerEventManager._

  // 当前Controller的状态，虽然是单线程模型，但是需要使用volatile保证对其它线程的可见性
  // 默认初始状态是「空闲状态」
  @volatile private var _state: ControllerState = ControllerState.Idle

  private val putLock = new ReentrantLock()

  /**
   * Controller 事件管理器核心变量，它是一个并发队列，
   * 存储来自ZookeeperWatch线程、KafakRequestHandler线程、定时任务线程和其它线程所产生的Controller事件，
   * 上面所说的是消费者，这个队列是构成消费者-生产者的交互对象。
   */
  private val queue = new LinkedBlockingQueue[QueuedEvent]

  // 单个线程，处理Controller事件的线程
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  // Controller事件管理器启动状态
  def state: ControllerState = _state

  // 启动Controller事件处理线程
  def start(): Unit = thread.start()

  // 关闭Controller事件处理线程
  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  /**
   * 将Controller事件放入任务队列 {@link ControllerEventManager.queue} 中
   *
   * @param event 待处理的Controller事件
   * @return
   */
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // #1 使用QueuedEvent对Controller事件进行包装，主要是丰富的相关的处理、等待处理完成等方法
    val queuedEvent = new QueuedEvent(event, time.milliseconds())

    // #2 加入任务队列
    queue.put(queuedEvent)

    // #3 返回包装对象QueuedEvent
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    preemptedEvents.forEach(_.preempt(processor))
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  /**
   * Controller 事件处理线程，Controller事件处理使用单线程模型，
   * 避免并发情况下造成代码复杂。我们可以对它进行监控，线程名是：controller-event-thread
   *
   * @param name
   */
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    /**
     * Controller 事件处理线程核心方法：
     * ① 从任务队列中取出Controller事件
     * ② 判断事件类型是否为ShutdownEventThread，如果是，什么也不做
     * ③
     */
    override def doWork(): Unit = {
      // #1 从事件队列中获取待处理的Controller事件（阻塞调用）
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // #2 如果是关闭线程事件，什么也不用做。关闭线程操作是由外部执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          // #3 获取事件状态
          _state = controllerEvent.state
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)

            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time {
                // #4 任务处理
                process()
              }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
