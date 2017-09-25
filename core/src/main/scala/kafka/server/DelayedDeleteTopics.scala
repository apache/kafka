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

package kafka.server

import org.apache.kafka.common.protocol.Errors

import scala.collection._

/**
  * The delete metadata maintained by the delayed delete operation
  */
case class DeleteTopicMetadata(topic: String, error: Errors)

/**
  * A delayed delete topics operation that can be created by the admin manager and watched
  * in the topic purgatory
  */
class DelayedDeleteTopics(delayMs: Long,
                          deleteMetadata: Seq[DeleteTopicMetadata],
                          adminManager: AdminManager,
                          responseCallback: Map[String, Errors] => Unit)
  extends DelayedOperation(delayMs) {

  /**
    * The operation can be completed if all of the topics not in error have been removed
    */
  override def tryComplete() : Boolean = {
    trace(s"Trying to complete operation for $deleteMetadata")

    // Ignore topics that already have errors
    val existingTopics = deleteMetadata.count { metadata => metadata.error == Errors.NONE && topicExists(metadata.topic) }

    if (existingTopics == 0) {
      trace("All topics have been deleted or have errors, completing the delayed operation")
      forceComplete()
    } else {
      trace(s"$existingTopics topics still exist, not completing the delayed operation")
      false
    }
  }

  /**
    * Check for partitions that still exist, update their error code and call the responseCallback
    */
  override def onComplete() {
    trace(s"Completing operation for $deleteMetadata")
    val results = deleteMetadata.map { metadata =>
      // ignore topics that already have errors
      if (metadata.error == Errors.NONE && topicExists(metadata.topic))
        (metadata.topic, Errors.REQUEST_TIMED_OUT)
      else
        (metadata.topic, metadata.error)
    }.toMap
    responseCallback(results)
  }

  override def onExpiration(): Unit = { }

  private def topicExists(topic: String): Boolean = {
    adminManager.metadataCache.contains(topic)
  }
}
