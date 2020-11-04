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

import java.util
import java.util.concurrent.CompletableFuture

import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.utils.{Logging, Mx4jLoader}
import org.apache.kafka.common.metrics.{JmxReporter, MetricsReporter}
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._

object KafkaServerManager extends Logging {
  def apply(config: KafkaConfig,
            time: Time = Time.SYSTEM,
            threadNamePrefix: Option[String] = None,
            kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()): KafkaServerManager = {
    val roles = config.processRoles
    var legacyBroker: Option[LegacyBroker] = None
    var kip500Broker: Option[Kip500Broker] = None
    var controller: Option[Kip500Controller] = None
    if (roles == null || roles.isEmpty) {
      info("Starting legacy broker.")
      legacyBroker = Some(new LegacyBroker(config, time, threadNamePrefix, kafkaMetricsReporters))
    } else {
      if (roles.asScala.distinct.length != roles.size()) {
        throw new RuntimeException(s"Duplicate role names found in roles config ${roles}")
      }
      if (config.controllerConnect.isEmpty) {
        throw new RuntimeException(s"You must specify a value for ${KafkaConfig.ControllerConnectProp}")
      }
      roles.asScala.foreach {
        case "broker" =>
          kip500Broker = Some(new Kip500Broker(config, time,
            threadNamePrefix, kafkaMetricsReporters))
        case "controller" =>
          controller = Some(new Kip500Controller(config, time, threadNamePrefix,
            kafkaMetricsReporters, CompletableFuture.completedFuture(config.controllerConnect)))
        case role =>
          throw new RuntimeException("Unknown process role " + role)
      }
      val bld = new StringBuilder
      var prefix = ""
      bld.append("Starting ")
      if (kip500Broker.isDefined) {
        bld.append("broker")
        prefix = " and "
      }
      if (controller.isDefined) {
        bld.append(prefix).append("controller")
      }
      bld.append(".")
      info(bld.toString)
    }
    new KafkaServerManager(config, legacyBroker, kip500Broker, controller)
  }
}

/**
 * Manages a set of servers within the Kafka process.
 */
class KafkaServerManager(val config: KafkaConfig,
                         val legacyBroker: Option[LegacyBroker],
                         val kip500Broker: Option[Kip500Broker],
                         val controller: Option[Kip500Controller]) extends Logging {
  private def legacyMode(): Boolean = legacyBroker.isDefined

  private def configureMetrics(): Unit = {
    KafkaYammerMetrics.INSTANCE.configure(config.originals)
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)
    val reporters = new util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)
    //val metricConfig =
    KafkaBroker.metricConfig(config)
//    val metricsContext = createKafkaMetricsContext()
//    metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)
  }

  def startup(): Unit = {
    if (!legacyMode()) {
      // TODO: we can probably configure this here even in legacy mode... eventually.
      configureMetrics()
      Mx4jLoader.maybeLoad()
    }
    legacyBroker.foreach(_.startup())
    kip500Broker.foreach(_.startup())
    controller.foreach(_.startup())
  }

  def shutdown(): Unit = {
    legacyBroker.foreach(_.shutdown())
    kip500Broker.foreach(_.shutdown())
    controller.foreach(_.shutdown())
  }

  def awaitShutdown(): Unit = {
    legacyBroker.foreach(_.awaitShutdown())
    kip500Broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

  def broker(): KafkaBroker = {
    List(legacyBroker, kip500Broker).flatten.head
  }
}