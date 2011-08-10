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

package kafka

import consumer.ConsumerConfig
import org.apache.log4j.Logger
import server.{KafkaConfig, KafkaServerStartable, KafkaServer}
import utils.Utils
import org.apache.log4j.jmx.LoggerDynamicMBean

object Kafka {
  private val logger = Logger.getLogger(Kafka.getClass)

  def main(args: Array[String]): Unit = {
    val kafkaLog4jMBeanName = "kafka:type=kafka.KafkaLog4j"
    Utils.swallow(logger.warn, Utils.registerMBean(new LoggerDynamicMBean(Logger.getRootLogger()), kafkaLog4jMBeanName))

    if(args.length != 1 && args.length != 2) {
      println("USAGE: java [options] " + classOf[KafkaServer].getSimpleName() + " server.properties [consumer.properties")
      System.exit(1)
    }
  
    try {
      var kafkaServerStartble: KafkaServerStartable = null
      val props = Utils.loadProps(args(0))
      val serverConfig = new KafkaConfig(props)
      if (args.length == 2) {
        val consumerConfig = new ConsumerConfig(Utils.loadProps(args(1)))
        kafkaServerStartble = new KafkaServerStartable(serverConfig, consumerConfig)
      }
      else
        kafkaServerStartble = new KafkaServerStartable(serverConfig)

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          kafkaServerStartble.shutdown
          kafkaServerStartble.awaitShutdown
        }
      });

      kafkaServerStartble.startup
      kafkaServerStartble.awaitShutdown
    }
    catch {
      case e => logger.fatal(e)
    }
    System.exit(0)
  }
}
