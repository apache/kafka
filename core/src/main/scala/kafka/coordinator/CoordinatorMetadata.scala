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

package kafka.coordinator

import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.threadsafe

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

/**
 * CoordinatorMetadata manages group and topic metadata.
 * It delegates all group logic to the callers.
 */
@threadsafe
private[coordinator] class CoordinatorMetadata(brokerId: Int) {

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */
  private val metadataLock = new ReentrantReadWriteLock()

  /**
   * These should be guarded by metadataLock
   */
  private val groups = new mutable.HashMap[String, GroupMetadata]

  def shutdown() {
    inWriteLock(metadataLock) {
      groups.clear()
    }
  }

  /**
   * Get the group associated with the given groupId, or null if not found
   */
  def getGroup(groupId: String) = {
    inReadLock(metadataLock) {
      groups.get(groupId).orNull
    }
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(groupId: String, protocolType: String) = {
    inWriteLock(metadataLock) {
      groups.getOrElseUpdate(groupId, new GroupMetadata(groupId, protocolType))
    }
  }

  /**
   * Remove all metadata associated with the group, including its topics
   * @param groupId the groupId of the group we are removing
   */
  def removeGroup(groupId: String) {
    inWriteLock(metadataLock) {
      if (!groups.contains(groupId))
        throw new IllegalArgumentException("Cannot remove non-existing group")
      groups.remove(groupId)
    }
  }

}
