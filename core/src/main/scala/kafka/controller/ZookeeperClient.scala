/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch}

import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

class ZookeeperClient(connectString: String, sessionTimeout: Int, sessionExpirationHandler: SessionExpirationHandler) {
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]()
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]()
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeout, ZookeeperClientWatcher)

  def handle(request: AsyncRequest): AsyncResponse = {
    handle(Seq(request)).head
  }

  def handle(requests: Seq[AsyncRequest]): Seq[AsyncResponse] = inReadLock(initializationLock) {
    import scala.collection.JavaConverters._
    val countDownLatch = new CountDownLatch(requests.size)
    val responseQueue = new ArrayBlockingQueue[AsyncResponse](requests.size)
    requests.foreach {
      case CreateRequest(path, data, acl, createMode, ctx) => zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
        override def processResult(rc: Int, path: String, ctx: Any, name: String) = {
          responseQueue.add(CreateResponse(rc, path, ctx, name))
          countDownLatch.countDown()
        }}, ctx)
      case DeleteRequest(path, version, ctx) => zooKeeper.delete(path, version, new VoidCallback {
        override def processResult(rc: Int, path: String, ctx: Any) = {
          responseQueue.add(DeleteResponse(rc, path, ctx))
          countDownLatch.countDown()
        }}, ctx)
      case ExistsRequest(path, ctx) => zooKeeper.exists(path, false, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(ExistsResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetDataRequest(path, ctx) => zooKeeper.getData(path, false, new DataCallback {
        override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) = {
          responseQueue.add(GetDataResponse(rc, path, ctx, data, stat))
          countDownLatch.countDown()
        }}, ctx)
      case SetDataRequest(path, data, version, ctx) => zooKeeper.setData(path, data, version, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(SetDataResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetACLRequest(path, ctx) => zooKeeper.getACL(path, null, new ACLCallback {
        override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
          responseQueue.add(GetACLResponse(rc, path, ctx, Option(acl).map(_.asScala).orNull, stat))
          countDownLatch.countDown()
        }}, ctx)
      case SetACLRequest(path, acl, version, ctx) => zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(SetACLResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, ctx)
      case GetChildrenRequest(path, ctx) => zooKeeper.getChildren(path, false, new Children2Callback {
        override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat) = {
          responseQueue.add(GetChildrenResponse(rc, path, ctx, Option(children).map(_.asScala).orNull, stat))
          countDownLatch.countDown()
        }}, ctx)
      case ExistsWithWatcherRequest(path) => zooKeeper.exists(path, true, new StatCallback {
        override def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = {
          responseQueue.add(ExistsResponse(rc, path, ctx, stat))
          countDownLatch.countDown()
        }}, null)
      case GetChildrenWithWatcherRequest(path) => zooKeeper.getChildren(path, true, new Children2Callback {
        override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat) = {
          responseQueue.add(GetChildrenResponse(rc, path, ctx, Option(children).map(_.asScala).orNull, stat))
          countDownLatch.countDown()
        }}, null)
    }
    countDownLatch.await()
    responseQueue.asScala.toSeq
  }

  def waitUntilConnectedOrExpired: Boolean = inLock(isConnectedOrExpiredLock) {
    var state = zooKeeper.getState
    while (!state.isConnected && state.isAlive) {
      isConnectedOrExpiredCondition.await()
      state = zooKeeper.getState
    }
    state.isConnected
  }

  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): ExistsResponse = {
    registerZNodeChangeHandlers(Seq(zNodeChangeHandler)).head
  }

  def registerZNodeChangeHandlers(handlers: Seq[ZNodeChangeHandler]): Seq[ExistsResponse] = {
    handlers.foreach(handler => zNodeChangeHandlers.put(handler.path, handler))
    val asyncRequests = handlers.map(handler => ExistsWithWatcherRequest(handler.path))
    handle(asyncRequests).asInstanceOf[Seq[ExistsResponse]]
  }

  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): GetChildrenResponse = {
    registerZNodeChildChangeHandlers(Seq(zNodeChildChangeHandler)).head
  }

  def registerZNodeChildChangeHandlers(handlers: Seq[ZNodeChildChangeHandler]): Seq[GetChildrenResponse] = {
    handlers.foreach(handler => zNodeChildChangeHandlers.put(handler.path, handler))
    val asyncRequests = handlers.map(handler => GetChildrenWithWatcherRequest(handler.path))
    handle(asyncRequests).asInstanceOf[Seq[GetChildrenResponse]]
  }

  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  def close(): Unit = inWriteLock(initializationLock) {
    zNodeChangeHandlers.clear()
    zNodeChildChangeHandlers.clear()
    zooKeeper.close()
  }

  private def initialize(): Unit = {
    if (!zooKeeper.getState.isAlive) {
      zNodeChangeHandlers.clear()
      zNodeChildChangeHandlers.clear()
      zooKeeper = new ZooKeeper(connectString, sessionTimeout, ZookeeperClientWatcher)
    }
  }

  private object ZookeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getPath == null) {
        inLock(isConnectedOrExpiredLock) {
          isConnectedOrExpiredCondition.signalAll()
        }
        if (event.getState == KeeperState.Expired) {
          inWriteLock(initializationLock) {
            sessionExpirationHandler.beforeInitializingSession
            initialize()
            sessionExpirationHandler.afterInitializingSession
          }
        }
      } else if (event.getType == EventType.NodeCreated) {
        Option(zNodeChangeHandlers.remove(event.getPath)).foreach(_.handleCreation)
      } else if (event.getType == EventType.NodeDeleted) {
        Option(zNodeChangeHandlers.remove(event.getPath)).foreach(_.handleDeletion)
      } else if (event.getType == EventType.NodeDataChanged) {
        Option(zNodeChangeHandlers.remove(event.getPath)).foreach(_.handleDataChange)
      } else if (event.getType == EventType.NodeChildrenChanged) {
        Option(zNodeChildChangeHandlers.remove(event.getPath)).foreach(_.handleChildChange)
      }
    }
  }

  private[this] case class ExistsWithWatcherRequest(path: String) extends AsyncRequest {
    val ctx = null
  }
  private[this] case class GetChildrenWithWatcherRequest(path: String) extends AsyncRequest {
    val ctx = null
  }
}

trait SessionExpirationHandler {
  def beforeInitializingSession: Unit
  def afterInitializingSession: Unit
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation: Unit
  def handleDeletion: Unit
  def handleDataChange: Unit
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange: Unit
}

sealed trait AsyncRequest {
  val path: String
  val ctx: Any
}
case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode, ctx: Any) extends AsyncRequest
case class DeleteRequest(path: String, version: Int, ctx: Any) extends AsyncRequest
case class ExistsRequest(path: String, ctx: Any) extends AsyncRequest
case class GetDataRequest(path: String, ctx: Any) extends AsyncRequest
case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Any) extends AsyncRequest
case class GetACLRequest(path: String, ctx: Any) extends AsyncRequest
case class SetACLRequest(path: String, acl: Seq[ACL], version: Int, ctx: Any) extends AsyncRequest
case class GetChildrenRequest(path: String, ctx: Any) extends AsyncRequest

sealed trait AsyncResponse {
  val rc: Int
  val path: String
  val ctx: Any
}
case class CreateResponse(rc: Int, path: String, ctx: Any, name: String) extends AsyncResponse
case class DeleteResponse(rc: Int, path: String, ctx: Any) extends AsyncResponse
case class ExistsResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetDataResponse(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) extends AsyncResponse
case class SetDataResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetACLResponse(rc: Int, path: String, ctx: Any, acl: Seq[ACL], stat: Stat) extends AsyncResponse
case class SetACLResponse(rc: Int, path: String, ctx: Any, stat: Stat) extends AsyncResponse
case class GetChildrenResponse(rc: Int, path: String, ctx: Any, children: Seq[String], stat: Stat) extends AsyncResponse