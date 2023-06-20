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

package kafka.network

import kafka.network.Processor.ListenerMetricTag

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import scala.collection.Map


/**
 * Interface for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
sealed trait ConnectionQuotaEntity {
  def sensorName: String
  def metricName: String
  def sensorExpiration: Long
  def metricTags: Map[String, String]

  val ConnectionRateSensorName = "Connection-Accept-Rate"
  val ConnectionRateMetricName = "connection-accept-rate"
  val TenantMetricTag = "tenant"
  val IpMetricTag = "ip"
  val InactiveSensorExpirationTimeSeconds = TimeUnit.HOURS.toSeconds(1)
  val ConnectionQuotaMetricName = "connection-tokens"
}

private case class ListenerQuotaEntity(listenerName: String) extends ConnectionQuotaEntity {
  override def sensorName: String = s"$ConnectionRateSensorName-$listenerName"
  override def sensorExpiration: Long = Long.MaxValue
  override def metricName: String = ConnectionRateMetricName
  override def metricTags: Map[String, String] = Map(ListenerMetricTag -> listenerName)
}

private case object BrokerQuotaEntity extends ConnectionQuotaEntity {
  override def sensorName: String = ConnectionRateSensorName
  override def sensorExpiration: Long = Long.MaxValue
  override def metricName: String = s"broker-$ConnectionRateMetricName"
  override def metricTags: Map[String, String] = Map.empty
}

private case class IpQuotaEntity(ip: InetAddress) extends ConnectionQuotaEntity {
  override def sensorName: String = s"$ConnectionRateSensorName-${ip.getHostAddress}"
  override def sensorExpiration: Long = InactiveSensorExpirationTimeSeconds
  override def metricName: String = ConnectionRateMetricName
  override def metricTags: Map[String, String] = Map(IpMetricTag -> ip.getHostAddress)
}
