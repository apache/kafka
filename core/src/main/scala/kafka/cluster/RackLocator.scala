/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import java.util.Properties

import kafka.server.KafkaConfig
import kafka.utils.CoreUtils
import org.I0Itec.zkclient.ZkClient

import scala.collection.Map

/**
 * Interface that defines API to get broker to rack mapping. This is used by
 * rack aware replica assignment. The implementation class can be passed as
 * rack.locator.class property at server start up or -rack-locator-class command
 * line argument for TopicCommand.
 *
 * @param zkClient ZkClient for the cluster that can be used to retrieve information
 *                 from ZooKeeper
 * @param props Properties used by implementation.
 */
abstract class RackLocator(val zkClient: ZkClient, val props: Properties) {
  def getRackInfo(): Map[Int, String]
}

class NoRack(zkClient: ZkClient, props: Properties) extends RackLocator(zkClient, props) {
  override def getRackInfo(): Map[Int, String] = {
    Map()
  }
}

object RackLocator {
  def create(zkClient: ZkClient, kafkaConfig: KafkaConfig): RackLocator = {
    create(zkClient, kafkaConfig.rackLocator, kafkaConfig.rackLocatorProps)
  }

  def create(zkClient: ZkClient, locatorClass: String = "", props: String = ""): RackLocator = {
    if (locatorClass == "") {
      new NoRack(zkClient, new Properties())
    } else {
      val properties = loadProperties(props)
      CoreUtils.createObject[RackLocator](locatorClass, zkClient, properties)
    }
  }

  private[cluster] def loadProperties(str: String): Properties = {
    val props = new Properties()
    if (!"".equals(str)) {
      val keyVals = str.split("\\s*,\\s*").map(s => s.split("\\s*=\\s*"))
      keyVals.map(pair => (pair(0), pair(1)))
        .toMap
        .foreach { keyVal => props.setProperty(keyVal._1, keyVal._2) }
    }
    props
  }
}
