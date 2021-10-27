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

package kafka.server.metadata

import org.apache.kafka.image.FeaturesDelta

import scala.collection.mutable

class MetadataVersionManager {
  @volatile var version: Short = -1

  private val listeners: mutable.Set[MetadataVersionChangeListener] = mutable.HashSet()

  def update(featuresDelta: FeaturesDelta, highestMetadataOffset: Long): Unit = {
    if (featuresDelta.metadataVersionChange().isPresent) {
      val prev = version
      val curr = featuresDelta.metadataVersionChange().get.max
      version = curr
      listeners.foreach { listener =>
        listener.apply(prev, curr)
      }
    }
  }

  def get(): Short = version

  def listen(listener: MetadataVersionChangeListener): Unit = {
    listeners.add(listener)
  }
}

trait MetadataVersionChangeListener {
  def apply(previousVersion: Short, currentVersion: Short): Unit
}