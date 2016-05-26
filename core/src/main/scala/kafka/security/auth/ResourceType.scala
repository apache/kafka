/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import kafka.common.{BaseEnum, KafkaException}
import org.apache.kafka.common.protocol.Errors

/**
 * ResourceTypes.
 */


sealed trait ResourceType extends BaseEnum { def errorCode: Short }

case object Cluster extends ResourceType {
  val name = "Cluster"
  val errorCode = Errors.CLUSTER_AUTHORIZATION_FAILED.code
}

case object Topic extends ResourceType {
  val name = "Topic"
  val errorCode = Errors.TOPIC_AUTHORIZATION_FAILED.code
}

case object Group extends ResourceType {
  val name = "Group"
  val errorCode = Errors.GROUP_AUTHORIZATION_FAILED.code
}


object ResourceType {

  def fromString(resourceType: String): ResourceType = {
    val rType = values.find(rType => rType.name.equalsIgnoreCase(resourceType))
    rType.getOrElse(throw new KafkaException(resourceType + " not a valid resourceType name. The valid names are " + values.mkString(",")))
  }

  def values: Seq[ResourceType] = List(Cluster, Topic, Group)
}
