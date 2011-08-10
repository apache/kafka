/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.net.URI
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.nio.channels.ClosedByInterruptException
import joptsimple._
import org.apache.log4j.Logger
import kafka.utils.Utils
import kafka.consumer.{ConsumerConfig, ConsumerConnector, Consumer}

abstract class ShutdownableThread(name: String) extends Thread(name) {
  def shutdown(): Unit  
}

/**
 * Performance test for the full zookeeper consumer
 */
object ConsumerPerformance {
  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val consumerPropsOpt = parser.accepts("props", "REQUIRED: Properties file with the consumer properties.")
                           .withRequiredArg
                           .describedAs("properties")
                           .ofType(classOf[String])
    val numThreadsOpt = parser.accepts("threads", "Number of processing threads.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(10)
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(100000)
    val sleepSecsOpt = parser.accepts("sleep", "Initial interval to wait before connecting.")
                           .withRequiredArg
                           .describedAs("secs")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(5)
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(topicOpt, consumerPropsOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val numThreads = options.valueOf(numThreadsOpt).intValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val propsFile = options.valueOf(consumerPropsOpt)
    val topic = options.valueOf(topicOpt)
    val printInterval = options.valueOf(reportingIntervalOpt).intValue
    val initialSleep = options.valueOf(sleepSecsOpt).intValue * 1000
    
    println("Starting consumer...")
    var totalNumMsgs = new AtomicLong(0)
    var totalNumBytes = new AtomicLong(0)
    
    val consumerConfig = new ConsumerConfig(Utils.loadProps(propsFile))
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)

    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> numThreads))
    var threadList = List[ShutdownableThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new ShutdownableThread("kafka-zk-consumer-" + i) {
          private val shutdownLatch = new CountDownLatch(1)

          def shutdown(): Unit = {
            interrupt
            shutdownLatch.await
          }

          override def run() {
            var totalBytesRead = 0L
            var nMessages = 0L
            val startMs = System.currentTimeMillis

            try {
              for (message <- streamList(i)) {
                nMessages += 1
                totalBytesRead += message.payloadSize
                if (nMessages % printInterval == 0) {
                  val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
                  printMessage(totalBytesRead, nMessages, elapsedSecs)
                }
              }
            }
            catch {
              case _: InterruptedException =>
              case _: ClosedByInterruptException =>
              case e => throw e
            }
            totalNumMsgs.addAndGet(nMessages)
            totalNumBytes.addAndGet(totalBytesRead)
            val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
            printMessage(totalBytesRead, nMessages, elapsedSecs)
            shutdownComplete
          }

          private def printMessage(totalBytesRead: Long, nMessages: Long, elapsedSecs: Double) = {
            logger.info("thread[" + i + "], nMsgs:" + nMessages + " bytes:" + totalBytesRead +
              " nMsgs/sec:" + (nMessages / elapsedSecs).formatted("%.2f") +
              " MB/sec:" + (totalBytesRead / elapsedSecs / (1024.0*1024.0)).formatted("%.2f"))

          }
          private def shutdownComplete() = shutdownLatch.countDown
        }

    logger.info("Sleeping for " + initialSleep / 1000 + " seconds.")
    Thread.sleep(initialSleep)
    logger.info("starting threads")
    for (thread <- threadList)
      thread.start

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        for (thread <- threadList)
          thread.shutdown

        try {
          consumerConnector.shutdown
        }
        catch {
          case _ =>
        }
        println("total nMsgs: " + totalNumMsgs)
        println("totalBytesRead " + totalNumBytes)
      }
    });

  }

}
