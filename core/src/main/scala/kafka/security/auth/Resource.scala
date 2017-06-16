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

object Resource {
  val Separator = ":"
  val ClusterResourceName = "kafka-cluster"
  val ClusterResource = new Resource(Cluster, Resource.ClusterResourceName)
  val ProducerIdResourceName = "producer-id"
  val WildCardResource = "*"

  def fromString(str: String): Resource = {
    str.split(Separator, 2) match {
      case Array(resourceType, name, _*) => new Resource(ResourceType.fromString(resourceType), name)
      case _ => throw new IllegalArgumentException("expected a string in format ResourceType:ResourceName but got " + str)
    }
  }
}

/**
 *
 * @param resourceType type of resource.
 * @param name name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 */
case class Resource(resourceType: ResourceType, name: String) {

  override def toString: String = {
    resourceType.name + Resource.Separator + name
  }
}

