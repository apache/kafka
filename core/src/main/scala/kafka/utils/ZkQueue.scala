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

import kafka.utils.ZkUtils._
import kafka.common.QueueFullException
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import java.util.concurrent.PriorityBlockingQueue
import java.util.Comparator

class ZkQueue(zkClient: ZkClient, path: String, size: Int) {
  // create the queue in ZK, if one does not exist
  makeSurePersistentPathExists(zkClient, path)
  val queueItems = new PriorityBlockingQueue[String](size, new ZkQueueComparator)
  var latestQueueItemPriority: Int = -1
  zkClient.subscribeChildChanges(path, new ZkQueueListener)

  // TODO: This API will be used by the leader to enqueue state change requests to the followers
  /**
   * Inserts the specified element into this priority queue. This method will never block. If the queue is full,
   * it will throw QueueFullException
   * @param item Item to add to the zookeeper queue
   * @returns The zookeeper location of item in the queue
   */
  def put(item: String): String = {
    // if queue is full, throw QueueFullException
    if(isFull)
      throw new QueueFullException("Queue is full. Item %s will be rejected".format(item))
    val queueLocation = createSequentialPersistentPath(zkClient, path + "/", item)
    debug("Added item %s to queue at location %s".format(item, queueLocation))
    queueLocation
  }

  /**
   * Reads all the items and their queue locations in this queue
   * @returns A list of (queue_location, item) pairs
   */
  def readAll(): Seq[(String, String)] = {
    val allItems = getChildren(zkClient, path).sorted
    allItems.size match {
      case 0 => Seq.empty[(String, String)]
      case _ => allItems.map { item =>
        // read the data and delete the node
        val queueLocation = path + "/" + item
        val data = ZkUtils.readData(zkClient, queueLocation)
        (item, data)
      }
    }
  }

  /**
   * Returns true if this zookeeper queue contains no elements.
   */
  def isEmpty: Boolean = (readAll().size == 0)

  // TODO: Implement the queue shrink operation if the queue is full, as part of create/delete topic
  /**
   * Returns true if this zookeeper queue contains number of items equal to the size of the queue
   */
  def isFull: Boolean = (readAll().size == size)

  /**
   * Retrieves but does not remove the head of this queue, waiting if necessary until an element becomes available.
   * @returns The location of the head and the head element in the zookeeper queue
   */
  def take(): (String, String) = {
    // take the element key
    val item = queueItems.take()
    val queueLocation = path + "/" + item
    val data = ZkUtils.readData(zkClient, queueLocation)
    (item, data)
  }

  /**
   * Removes a single instance of the specified element from this queue, if it is present. More formally, removes an
   * element e such that o.equals(e), if this queue contains one or more such elements. Returns true if this queue
   * contained the specified element (or equivalently, if this queue changed as a result of the call).
   * @param queueItem A tuple where the first element is the location of the item as returned by the take() API and the
   * second element is the queue item to be removed
   */
  def remove(queueItem: (String, String)): Boolean = {
    val queueLocation = path + "/" + queueItem._1
    // we do not want to remove items from the queue if they were not read
    assert(!queueItems.contains(queueItem._1), "Attempt to remove unconsumed item %s from the queue".format(queueItem))
    ZkUtils.deletePath(zkClient, queueLocation)
  }

  class ZkQueueListener extends IZkChildListener with Logging {

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      debug("ZkQueue listener fired for queue %s with children %s and latest queue item priority %d"
        .format(path, curChilds.toString, latestQueueItemPriority))
      import scala.collection.JavaConversions._
      val outstandingRequests = asBuffer(curChilds).sortWith((req1, req2) => req1.toInt < req2.toInt)
      outstandingRequests.foreach { req =>
        val queueItemPriority = req.toInt
        if(queueItemPriority > latestQueueItemPriority) {
          latestQueueItemPriority = queueItemPriority
          queueItems.add(req)
          debug("Added item %s to queue %s".format(req, path))
        }
      }
    }
  }

  class ZkQueueComparator extends Comparator[String] {
    def compare(element1: String, element2: String): Int = {
      element1.toInt - element2.toInt
    }
  }
}