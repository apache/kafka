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
package kafka.security.auth

import kafka.common.KafkaException
import org.apache.kafka.common.resource.{ResourceNameType, ResourcePattern}

object Resource {
  val Separator = ":"
  val ClusterResourceName = "kafka-cluster"
  val ClusterResource = Resource(Cluster, Resource.ClusterResourceName, ResourceNameType.LITERAL)
  val ProducerIdResourceName = "producer-id"
  val WildCardResource = "*"

  def fromString(str: String): Resource = {
    ResourceType.values.find(resourceType => str.startsWith(resourceType.name + Separator)) match {
      case None => throw new KafkaException("Invalid resource string: '" + str + "'")
      case Some(resourceType) =>
        val remaining = str.substring(resourceType.name.length + 1)

        ResourceNameType.values.find(nameType => remaining.startsWith(nameType.name + Separator)) match {
          case Some(nameType) =>
            val name = remaining.substring(nameType.name.length + 1)
            Resource(resourceType, name, nameType)

          case None =>
            Resource(resourceType, remaining, ResourceNameType.LITERAL)
        }
    }
  }
}

/**
 *
 * @param resourceType non-null type of resource.
 * @param name non-null name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 * @param nameType non-null type of resource name: literal, prefixed, etc.
 */
case class Resource(resourceType: ResourceType, name: String, nameType: ResourceNameType) {

  if (nameType == ResourceNameType.ANY)
    throw new IllegalArgumentException("nameType must not be ANY")

  if (nameType == ResourceNameType.UNKNOWN)
    throw new IllegalArgumentException("nameType must not be UNKNOWN")

  /**
    * Create an instance of this class with the provided parameters.
    * Resource name type would default to ResourceNameType.LITERAL.
    *
    * @param resourceType non-null resource type
    * @param name         non-null resource name
    * @deprecated Since 2.0, use [[kafka.security.auth.Resource(ResourceType, String, ResourceNameType)]]
    */
  @deprecated("Use Resource(ResourceType, String, ResourceNameType", "Since 2.0")
  def this(resourceType: ResourceType, name: String) {
    this(resourceType, name, ResourceNameType.LITERAL)
  }

  def toPattern: ResourcePattern = {
    new ResourcePattern(resourceType.toJava, name, nameType)
  }

  override def toString: String = {
    resourceType.name + Resource.Separator + nameType + Resource.Separator + name
  }
}

