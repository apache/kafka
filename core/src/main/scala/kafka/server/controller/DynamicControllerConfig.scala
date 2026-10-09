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

import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.controller.util.ControllerListenerReconfigurable

import java.util.concurrent.CopyOnWriteArrayList
import scala.jdk.CollectionConverters._

/**
 * Controller-side dynamic config manager.
 * Owns the current effective config map, the registry of reconfigurables,
 * and dispatches config updates on metadata replay.
 */
class DynamicControllerConfig(
  config: KafkaConfig
) extends Logging {

  private val reconfigurables = new CopyOnWriteArrayList[ControllerListenerReconfigurable]()
  @volatile private var currentConfig: Map[String, String] = Map.empty

  def addReconfigurable(reconfigurable: ControllerListenerReconfigurable): Unit = {
    reconfigurables.add(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: ControllerListenerReconfigurable): Unit = {
    reconfigurables.remove(reconfigurable)
  }

  /**
   * Apply a new effective config map from the metadata log.
   * This is called by ControllerDynamicConfigPublisher on each metadata update.
   */
  def apply(newConfig: java.util.Map[String, String]): Unit = {
    val newConfigScala = newConfig.asScala.toMap

    if (newConfigScala != currentConfig) {
      info(s"Applying controller config update: ${newConfigScala.size} configs")

      val iterator = reconfigurables.iterator()
      while (iterator.hasNext) {
        val reconfigurable = iterator.next()
        try {
          reconfigurable.validateReconfiguration(newConfig)
          reconfigurable.reconfigure(newConfig)
        } catch {
          case e: Exception =>
            error(s"Controller dynamic config update rejected for one reconfigurable; " +
                  s"previous configuration retained. Fix and re-alter.", e)
        }
      }
      currentConfig = newConfigScala
    }
  }

  def currentValue(key: String): Option[String] = {
    currentConfig.get(key)
  }
}
