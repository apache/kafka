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

package kafka.utils

import org.I0Itec.zkclient.ZkClient

/**
 * This class implements a ZK based exclusive reeentrant lock, according to classic recipe.
 * After Curator integration this class should be replaced by a respective recipe in Curator.
 */
class ZKLock {

  private[this] var lockPath: String = null
  private[this] var childLockPath: String = null
  private[this] var zkClient: ZkClient = null
  private[this] var backoffMs: Int = 0
  private[this] val DEFAULT_BACKOFF_MS: Int = 500

  @volatile var holdsLock: Boolean = false

  def this(zkClient: ZkClient, path: String, backoff: Option[Int] = None) {
    this()

    if (zkClient == null)
      throw new IllegalArgumentException("zkClient cannot be null")
    this.zkClient = zkClient

    if (path == null || path.isEmpty())
      throw new IllegalArgumentException("Invalid lock path: %s".format(path))
    this.lockPath = path

    this.backoffMs = backoff match {
      case Some(x) => x
      case None => DEFAULT_BACKOFF_MS
    }
  }

  @throws(classOf[InterruptedException])
  def acquire(): Unit = {
    this.synchronized {
      if (holdsLock)
        return

      if (!ZkUtils.pathExists(zkClient, lockPath))
        throw new RuntimeException("Persistent lock path '%s' doesn't exists".format(lockPath))

      var acquired: Boolean = false
      do {
        acquired = true

        childLockPath = ZkUtils.createEphemeralSequential(zkClient, lockPath + "/exclusive-lock-", "")
        val candidateId: Long = childLockPath.substring(childLockPath.lastIndexOf("-") + 1).toLong

        val children = ZkUtils.getChildren(zkClient, lockPath)
        for (child <- children) {
          val childId: Long = child.substring(child.lastIndexOf("-") + 1).toLong
          if (childId < candidateId)
            acquired = false
        }

        if (!acquired) {
          ZkUtils.deletePath(zkClient, childLockPath)
          childLockPath = null
          Thread.sleep(backoffMs)
        }
      } while (!acquired)
      holdsLock = acquired
    }

  }

  def release(): Unit = {
    this.synchronized {
      if (!holdsLock)
        throw new IllegalStateException("Locking is not held for path '%s'".format(childLockPath))
      ZkUtils.deletePath(zkClient, childLockPath)
    }
  }
}
