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

package kafka.server

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._

object KafkaServerManager {
  def apply(config: KafkaConfig,
            time: Time = Time.SYSTEM,
            threadNamePrefix: Option[String] = None,
            kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()): KafkaServerManager = {
    val roles = config.processRoles
    var legacyBroker: Option[LegacyBroker] = None
    var kip500Broker: Option[Kip500Broker] = None
    var controller: Option[Kip500Controller] = None
    if (roles == null || roles.isEmpty) {
      legacyBroker = Some(new LegacyBroker(config, time, threadNamePrefix, kafkaMetricsReporters))
    } else {
      if (roles.asScala.distinct.length != roles.size()) {
        throw new RuntimeException(s"Duplicate role names found in roles config ${roles}")
      }
      roles.asScala.foreach(role => role match {
        case "broker" =>
          kip500Broker = Some(new Kip500Broker(config, time,
            threadNamePrefix, kafkaMetricsReporters))
        case "controller" =>
          controller = Some(new Kip500Controller(config, time, threadNamePrefix,
            kafkaMetricsReporters))
        case _ =>
          throw new RuntimeException("Unknown process role " + role)
      })
    }
    new KafkaServerManager(legacyBroker, kip500Broker, controller)
  }
}

/**
 * Manages a set of servers within the Kafka process.
 */
class KafkaServerManager(val legacyBroker: Option[LegacyBroker],
                         val kip500Broker: Option[Kip500Broker],
                         val controller: Option[Kip500Controller]) extends Logging {
  def startup(): Unit = {
    legacyBroker.foreach(_.startup())
    kip500Broker.foreach(_.startup())
  }

  def shutdown(): Unit = {
    legacyBroker.foreach(_.shutdown())
    kip500Broker.foreach(_.shutdown())
  }

  def awaitShutdown(): Unit = {
    legacyBroker.foreach(_.awaitShutdown())
    kip500Broker.foreach(_.awaitShutdown())
  }

  def broker(): KafkaBroker = {
    List(legacyBroker, kip500Broker).flatten.head
  }
}