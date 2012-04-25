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

import kafka.message.Message
import joptsimple.OptionParser
import kafka.utils.{Utils, Logging}
import kafka.producer.{ProducerData, ProducerConfig, Producer}
import scala.collection.JavaConversions._
import java.util.concurrent.CountDownLatch
import kafka.consumer._


object MirrorMaker extends Logging {

  def main(args: Array[String]) {
    
    info ("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Consumer config to consume from a source cluster. " +
      "You may specify multiple of these.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val numProducersOpt = parser.accepts("num.producers",
      "Number of producer instances")
      .withRequiredArg()
      .describedAs("Number of producers")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    
    val numStreamsOpt = parser.accepts("num.streams",
      "Number of consumption streams.")
      .withRequiredArg()
      .describedAs("Number of threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    
    val whitelistOpt = parser.accepts("whitelist",
      "Whitelist of topics to mirror.")
      .withRequiredArg()
      .describedAs("Java regex (String)")
      .ofType(classOf[String])

    val blacklistOpt = parser.accepts("blacklist",
            "Blacklist of topics to mirror.")
            .withRequiredArg()
            .describedAs("Java regex (String)")
            .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print this message.")

    val options = parser.parse(args : _*)

    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    Utils.checkRequiredArgs(
      parser, options, consumerConfigOpt, producerConfigOpt)
    if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
      println("Exactly one of whitelist or blacklist is required.")
      System.exit(1)
    }

    val numStreams = options.valueOf(numStreamsOpt)

    val producers = (1 to options.valueOf(numProducersOpt).intValue()).map(_ => {
      val config = new ProducerConfig(
        Utils.loadProps(options.valueOf(producerConfigOpt)))
      new Producer[Null, Message](config)
    })

    val threads = {
      val connectors = options.valuesOf(consumerConfigOpt).toList
              .map(cfg => new ConsumerConfig(Utils.loadProps(cfg.toString)))
              .map(new ZookeeperConsumerConnector(_))

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          connectors.foreach(_.shutdown())
          producers.foreach(_.close())
        }
      })

      val filterSpec = if (options.has(whitelistOpt))
        new Whitelist(options.valueOf(whitelistOpt))
      else
        new Blacklist(options.valueOf(blacklistOpt))

      val streams =
        connectors.map(_.createMessageStreamsByFilter(filterSpec, numStreams.intValue()))

      streams.flatten.zipWithIndex.map(streamAndIndex => {
        new MirrorMakerThread(streamAndIndex._1, producers, streamAndIndex._2)
      })
    }

    threads.foreach(_.start())

    threads.foreach(_.awaitShutdown())
  }

  class MirrorMakerThread(stream: KafkaStream[Message],
                          producers: Seq[Producer[Null, Message]],
                          threadId: Int)
          extends Thread with Logging {

    private val shutdownLatch = new CountDownLatch(1)
    private val threadName = "mirrormaker-" + threadId
    private val producerSelector = Utils.circularIterator(producers)

    this.setName(threadName)

    override def run() {
      try {
        for (msgAndMetadata <- stream) {
          val producer = producerSelector.next()
          val pd = new ProducerData[Null, Message](
            msgAndMetadata.topic, msgAndMetadata.message)
          producer.send(pd)
        }
      }
      catch {
        case e =>
          fatal("%s stream unexpectedly exited.", e)
      }
      finally {
        shutdownLatch.countDown()
        info("Stopped thread %s.".format(threadName))
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
      }
      catch {
        case e: InterruptedException => fatal(
          "Shutdown of thread %s interrupted. This might leak data!"
                  .format(threadName))
      }
    }
  }
}

