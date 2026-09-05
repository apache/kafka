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

package kafka.server.controller

import kafka.utils.Logging
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ChannelBuilder, ListenerName, SslChannelBuilder}
import org.apache.kafka.controller.util.ControllerListenerReconfigurable

import java.util
import scala.jdk.CollectionConverters._

/**
 * Wraps a SslChannelBuilder and implements ControllerListenerReconfigurable
 * by delegating to the existing SslChannelBuilder.reconfigure() method.
 */
class DynamicControllerSslListener(
  channelBuilder: ChannelBuilder,
  listenerName: ListenerName
) extends ControllerListenerReconfigurable with Logging {

  private val sslChannelBuilder: Option[SslChannelBuilder] = channelBuilder match {
    case ssl: SslChannelBuilder => Some(ssl)
    case _ => None
  }

  if (sslChannelBuilder.isEmpty) {
    throw new IllegalArgumentException(s"ChannelBuilder for listener $listenerName is not an SslChannelBuilder")
  }

  override def reconfigurableConfigs(): util.Set[String] = {
    sslChannelBuilder.get.reconfigurableConfigs()
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    val listenerConfigs = filterConfigsForListener(configs)
    if (!listenerConfigs.isEmpty) {
      sslChannelBuilder.get.validateReconfiguration(listenerConfigs)
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val listenerConfigs = filterConfigsForListener(configs)
    if (!listenerConfigs.isEmpty) {
      info(s"Reconfiguring SSL for listener $listenerName with ${listenerConfigs.size()} configs")
      sslChannelBuilder.get.reconfigure(listenerConfigs)
    }
  }

  /**
   * Filter configs to only those relevant for this listener.
   * Accepts bare ssl.* keys and listener.name.<this-listener>.ssl.* keys.
  */
  private def filterConfigsForListener(configs: util.Map[String, _]): util.Map[String, AnyRef] = {
    val result = new util.HashMap[String, AnyRef]()
    val listenerPrefix = s"listener.name.${listenerName.value().toLowerCase}."

    configs.asScala.foreach { case (key, value) =>
      val lowerKey = key.toLowerCase
      if (lowerKey.startsWith("ssl.")) {
        result.put(key, wrapIfPassword(key, value))
      } else if (lowerKey.startsWith(listenerPrefix)) {
        val configKey = key.substring(listenerPrefix.length)
        result.put(configKey, wrapIfPassword(configKey, value))
      }
    }

    result
  }

  /**
   * SslFactory's reconfigurable validator/applier casts password values to
   * `org.apache.kafka.common.config.types.Password`.
   */
  private def wrapIfPassword(key: String, value: Any): AnyRef = value match {
    case p: Password => p
    case s: String if key.toLowerCase.contains("password") => new Password(s)
    case other => other.asInstanceOf[AnyRef]
  }
}
