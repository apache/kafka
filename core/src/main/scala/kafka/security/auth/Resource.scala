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

import java.util.Objects
import org.apache.kafka.common.resource.{Resource => JResource}

object Resource {
  val ClusterResourceName = "kafka-cluster"
  val ClusterResource = new Resource(Cluster, Resource.ClusterResourceName, Literal)
  val ProducerIdResourceName = "producer-id"
  val WildCardResource = "*"
}

/**
 *
 * @param resourceType non-null type of resource.
 * @param name non-null name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 * @param resourceNameType non-null type of resource name: literal, prefixed, etc.
 */
case class Resource(resourceType: ResourceType, name: String, resourceNameType: ResourceNameType) {

  Objects.requireNonNull(resourceType, "resourceType")
  Objects.requireNonNull(name, "name")
  Objects.requireNonNull(resourceNameType, "resourceNameType")

  /**
    * Create an instance of this class with the provided parameters.
    * Resource name type would default to ResourceNameType.LITERAL.
    *
    * @param resourceType non-null resource type
    * @param name         non-null resource name
    * @deprecated Since 2.0, use [[kafka.security.auth.Resource(ResourceType, String, ResourceNameType)]]
    */
  @deprecated("Use Resource(ResourceType, String, ResourceNameType")
  def this(resourceType: ResourceType, name: String) {
    this(resourceType, name, Literal)
  }

  def toJava: JResource = {
    new JResource(resourceType.toJava, name, resourceNameType.toJava)
  }
}

