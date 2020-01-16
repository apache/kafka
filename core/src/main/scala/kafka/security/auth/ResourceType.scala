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
import org.apache.kafka.common.resource.{ResourceType => JResourceType}

sealed trait ResourceType extends BaseEnum with Ordered[ ResourceType ] {
  def error: Errors
  def toJava: JResourceType
  // this method output will not include "All" Operation type
  def supportedOperations: Set[Operation]

  override def compare(that: ResourceType): Int = this.name compare that.name
}

case object Topic extends ResourceType {
  val name = "Topic"
  val error = Errors.TOPIC_AUTHORIZATION_FAILED
  val toJava = JResourceType.TOPIC
  val supportedOperations = Set(Read, Write, Create, Describe, Delete, Alter, DescribeConfigs, AlterConfigs)
}

case object Group extends ResourceType {
  val name = "Group"
  val error = Errors.GROUP_AUTHORIZATION_FAILED
  val toJava = JResourceType.GROUP
  val supportedOperations = Set(Read, Describe, Delete)
}

case object Cluster extends ResourceType {
  val name = "Cluster"
  val error = Errors.CLUSTER_AUTHORIZATION_FAILED
  val toJava = JResourceType.CLUSTER
  val supportedOperations = Set(Create, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, Alter, Describe)
}

case object TransactionalId extends ResourceType {
  val name = "TransactionalId"
  val error = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
  val toJava = JResourceType.TRANSACTIONAL_ID
  val supportedOperations = Set(Describe, Write)
}

case object DelegationToken extends ResourceType {
  val name = "DelegationToken"
  val error = Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED
  val toJava = JResourceType.DELEGATION_TOKEN
  val supportedOperations : Set[Operation] = Set(Describe)
}

object ResourceType {

  def fromString(resourceType: String): ResourceType = {
    val rType = values.find(rType => rType.name.equalsIgnoreCase(resourceType))
    rType.getOrElse(throw new KafkaException(resourceType + " not a valid resourceType name. The valid names are " + values.mkString(",")))
  }

  def values: Seq[ResourceType] = List(Topic, Group, Cluster, TransactionalId, DelegationToken)

  def fromJava(operation: JResourceType): ResourceType = fromString(operation.toString.replaceAll("_", ""))
}
