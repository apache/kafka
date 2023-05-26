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

import com.yammer.metrics.core.Timer

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import scala.collection._

//保存一些字符串常量，比如线程名字。
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

//Controller 端的事件处理器接口。
//事件处理器接口，目前只有 KafkaController 实现了这个接口。
trait ControllerEventProcessor {
  def process(event: ControllerEvent): Unit //接收一个 Controller 事件，并进行处理。它是实现 Controller 事件处理的主力方法
  def preempt(event: ControllerEvent): Unit //接收一个 Controller 事件，并抢占队列之前的事件进行优先处理。Kafka 使用它实现某些高优先级事件的抢占处理，目前在源码中只有两类事件（ShutdownEventThread 和 Expire）需要抢占式处理，出镜率不是很高。
}

/**
 *  事件队列上的事件对象。
 *  每个QueuedEvent定义了两个字段
 *  event: ControllerEvent类，表示Controller事件
 *  enqueueTimeMs：表示Controller事件被放入到事件队列的时间戳
 *
 *  每个 QueuedEvent 对象实例都裹挟了一个 ControllerEvent。
 *  另外，每个 QueuedEvent 还定义了 process、preempt 和 awaitProcessing 方法，
 *  分别表示处理事件、以抢占方式处理事件，以及等待事件处理。
 */
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  // 标识事件是否开始被处理
  // Kafka 源码非常喜欢用 CountDownLatch 来做各种条件控制，比如用于侦测线程是否成功启动、成功关闭，
  // 在这里，QueuedEvent 使用它的唯一目的，是确保 Expire 事件在建立 ZooKeeper 会话前被处理。
  val processingStarted = new CountDownLatch(1)
  // 标识事件是否被处理过
  val spent = new AtomicBoolean(false)

  /**
   * 处理事件
   * 方法首先会判断该事件是否已经被处理过，如果是，就直接返回；
   * 如果不是，就调用 ControllerEventProcessor 的 process 方法处理事件。
   * @param processor
   */
  def process(processor: ControllerEventProcessor): Unit = {
    // 若已经被处理过，直接返回
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    // 调用ControllerEventProcessor的process方法处理事件
    processor.process(event)
  }

  // 抢占式处理事件
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  // 阻塞等待事件被处理完成
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

//事件处理器，用于创建和管理 ControllerEventThread
//ControllerEventManager 的伴生类，主要用于创建和管理事件处理线程和事件队列。
// 这个类中定义了重要的 ControllerEventThread 线程类，还有一些其他重要方法
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, Timer],
                             eventQueueTimeTimeoutMs: Long = 300000) {
  import ControllerEventManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = metricsGroup.newHistogram(EventQueueTimeMetricName)

  metricsGroup.newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      metricsGroup.removeMetric(EventQueueTimeMetricName)
      metricsGroup.removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 构建QueuedEvent实例
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 插入到事件队列
    queue.put(queuedEvent)
    // 返回新建QueuedEvent实例
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    // 优先处理抢占式事件
    preemptedEvents.forEach(_.preempt(processor))
    // 调用上面的put方法将给定事件插入到事件队列
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  //专属的事件处理线程，唯一的作用是处理不同种类的 ControllerEvent。这个类是 ControllerEventManager 类内部定义的线程类。
  //这个类就是一个普通的线程类，继承了 ShutdownableThread 基类，而后者是 Kafka 为很多线程类定义的公共父类。
  class ControllerEventThread(name: String)
    extends ShutdownableThread(
      name, false, s"[ControllerEventThread controllerId=$controllerId] ")
      with Logging {

    logIdent = logPrefix

    /**
     * 首先是调用 LinkedBlockingQueue 的 take 方法，去获取待处理的 QueuedEvent 对象实例。
     * 注意，这里用的是 take 方法，这说明，如果事件队列中没有 QueuedEvent，那么，ControllerEventThread 线程将一直处于阻塞状态，直到事件队列上插入了新的待处理事件。
     * 一旦拿到 QueuedEvent 事件后，线程会判断是否是 ShutdownEventThread 事件。
     * 当 ControllerEventManager 关闭时，会显式地向事件队列中塞入 ShutdownEventThread，表明要关闭 ControllerEventThread 线程。
     * 如果是该事件，那么 ControllerEventThread 什么都不用做，毕竟要关闭这个线程了。
     * 相反地，如果是其他的事件，就调用 QueuedEvent 的 process 方法执行对应的处理逻辑，同时计算事件被处理的速率。
     */
    override def doWork(): Unit = {
      // 从事件队列中获取待处理的Controller事件，否则等待
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state
          // 更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            //该 process 方法底层调用的是 ControllerEventProcessor 的 process 方法
            def process(): Unit = dequeued.process(processor)
            // 处理事件，同时计算处理速率
            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time(() => process())
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
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
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
