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

package kafka.consumer

import scala.collection.JavaConverters._
import kafka.utils.{ZkUtils, Logging}
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener}
import org.apache.zookeeper.Watcher.Event.KeeperState

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ZookeeperTopicEventWatcher(val zkUtils: ZkUtils,
    val eventHandler: TopicEventHandler[String]) extends Logging {

  val lock = new Object()

  startWatchingTopicEvents()

  private def startWatchingTopicEvents() {
    val topicEventListener = new ZkTopicEventListener()
    zkUtils.makeSurePersistentPathExists(ZkUtils.BrokerTopicsPath)

    zkUtils.subscribeStateChanges(new ZkSessionExpireListener(topicEventListener))

    val topics = zkUtils.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener).getOrElse {
      throw new AssertionError(s"Expected ${ZkUtils.BrokerTopicsPath} to exist, but it does not. ")
    }

    // call to bootstrap topic list
    topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics.asJava)
  }

  private def stopWatchingTopicEvents() { zkUtils.unsubscribeAll() }

  def shutdown() {
    lock.synchronized {
      info("Shutting down topic event watcher.")
      if (zkUtils != null) {
        stopWatchingTopicEvents()
      }
      else {
        warn("Cannot shutdown since the embedded zookeeper client has already closed.")
      }
    }
  }

  class ZkTopicEventListener extends IZkChildListener {

    @throws[Exception]
    def handleChildChange(parent: String, children: java.util.List[String]) {
      lock.synchronized {
        try {
          if (zkUtils != null) {
            val latestTopics = zkUtils.getChildren(ZkUtils.BrokerTopicsPath)
            debug("all topics: %s".format(latestTopics))
            eventHandler.handleTopicEvent(latestTopics)
          }
        }
        catch {
          case e: Throwable =>
            error("error in handling child changes", e)
        }
      }
    }

  }

  class ZkSessionExpireListener(val topicEventListener: ZkTopicEventListener)
    extends IZkStateListener {

    @throws[Exception]
    def handleStateChanged(state: KeeperState) { }

    @throws[Exception]
    def handleNewSession() {
      lock.synchronized {
        if (zkUtils != null) {
          info("ZK expired: resubscribing topic event listener to topic registry")
          zkUtils.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener)
        }
      }
    }

    override def handleSessionEstablishmentError(error: Throwable): Unit = {
      //no-op ZookeeperConsumerConnector should log error.
    }
  }
}

