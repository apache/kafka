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

import kafka.server.metadata.{FeatureCacheUpdateException, ZkMetadataCache}

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import kafka.utils.Logging
import kafka.zk.{FeatureZNode, FeatureZNodeStatus, KafkaZkClient, ZkVersion}
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.server.util.ShutdownableThread

import scala.concurrent.TimeoutException

/**
 * Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
 * is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
 * to the latest features read from ZK. The cache updates are serialized through a single
 * notification processor thread.
 *
 * This updates the features cached in ZkMetadataCache
 *
 * @param finalizedFeatureCache   the finalized feature cache
 * @param zkClient                the Zookeeper client
 */
class FinalizedFeatureChangeListener(private val finalizedFeatureCache: ZkMetadataCache,
                                     private val zkClient: KafkaZkClient) extends Logging {

  /**
   * Helper class used to update the FinalizedFeatureCache.
   *
   * @param featureZkNodePath   the path to the ZK feature node to be read
   * @param maybeNotifyOnce     an optional latch that can be used to notify the caller when an
   *                            updateOrThrow() operation is over
   */
  private class FeatureCacheUpdater(featureZkNodePath: String, maybeNotifyOnce: Option[CountDownLatch]) {

    def this(featureZkNodePath: String) = this(featureZkNodePath, Option.empty)

    /**
     * Updates the feature cache in FinalizedFeatureCache with the latest features read from the
     * ZK node in featureZkNodePath. If the cache update is not successful, then, a suitable
     * exception is raised.
     *
     * NOTE: if a notifier was provided in the constructor, then, this method can be invoked exactly
     * once successfully. A subsequent invocation will raise an exception.
     *
     * @throws   IllegalStateException, if a non-empty notifier was provided in the constructor, and
     *           this method is called again after a successful previous invocation.
     * @throws   FeatureCacheUpdateException, if there was an error in updating the
     *           FinalizedFeatureCache.
     */
    def updateLatestOrThrow(): Unit = {
      maybeNotifyOnce.foreach(notifier => {
        if (notifier.getCount != 1) {
          throw new IllegalStateException(
            "Can not notify after updateLatestOrThrow was called more than once successfully.")
        }
      })

      debug(s"Reading feature ZK node at path: $featureZkNodePath")
      val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(featureZkNodePath)

      // There are 4 cases:
      //
      // (empty dataBytes, valid version)       => The empty dataBytes will fail FeatureZNode deserialization.
      //                                           FeatureZNode, when present in ZK, can not have empty contents.
      // (non-empty dataBytes, valid version)   => This is a valid case, and should pass FeatureZNode deserialization
      //                                           if dataBytes contains valid data.
      // (empty dataBytes, unknown version)     => This is a valid case, and this can happen if the FeatureZNode
      //                                           does not exist in ZK.
      // (non-empty dataBytes, unknown version) => This case is impossible, since, KafkaZkClient.getDataAndVersion
      //                                           API ensures that unknown version is returned only when the
      //                                           ZK node is absent. Therefore dataBytes should be empty in such
      //                                           a case.
      if (version == ZkVersion.UnknownVersion) {
        info(s"Feature ZK node at path: $featureZkNodePath does not exist")
        finalizedFeatureCache.clearFeatures()
      } else {
        var maybeFeatureZNode: Option[FeatureZNode] = Option.empty
        try {
          maybeFeatureZNode = Some(FeatureZNode.decode(mayBeFeatureZNodeBytes.get))
        } catch {
          case e: IllegalArgumentException => {
            error(s"Unable to deserialize feature ZK node at path: $featureZkNodePath", e)
            finalizedFeatureCache.clearFeatures()
          }
        }
        maybeFeatureZNode.foreach(featureZNode => {
          featureZNode.status match {
            case FeatureZNodeStatus.Disabled => {
              info(s"Feature ZK node at path: $featureZkNodePath is in disabled status.")
              finalizedFeatureCache.clearFeatures()
            }
            case FeatureZNodeStatus.Enabled => {
              finalizedFeatureCache.updateFeaturesOrThrow(featureZNode.features.toMap, version)
            }
            case _ => throw new IllegalStateException(s"Unexpected FeatureZNodeStatus found in $featureZNode")
          }
        })
      }

      maybeNotifyOnce.foreach(notifier => notifier.countDown())
    }

    /**
     * Waits until at least a single updateLatestOrThrow completes successfully. This method returns
     * immediately if an updateLatestOrThrow call had already completed successfully.
     *
     * @param waitTimeMs   the timeout for the wait operation
     *
     * @throws             TimeoutException if the wait can not be completed in waitTimeMs
     *                     milli seconds
     */
    def awaitUpdateOrThrow(waitTimeMs: Long): Unit = {
      maybeNotifyOnce.foreach(notifier => {
        if (!notifier.await(waitTimeMs, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(
            s"Timed out after waiting for ${waitTimeMs}ms for FeatureCache to be updated.")
        }
      })
    }
  }

  /**
   * A shutdownable thread to process feature node change notifications that are populated into the
   * queue. If any change notification can not be processed successfully (unless it is due to an
   * interrupt), the thread treats it as a fatal event and triggers Broker exit.
   *
   * @param name   name of the thread
   */
  private class ChangeNotificationProcessorThread(name: String) extends ShutdownableThread(name) with Logging {

    this.logIdent = logPrefix

    override def doWork(): Unit = {
      try {
        queue.take.updateLatestOrThrow()
      } catch {
        case ie: InterruptedException =>
          // While the queue is empty and this thread is blocking on taking an item from the queue,
          // a concurrent call to FinalizedFeatureChangeListener.close() could interrupt the thread
          // and cause an InterruptedException to be raised from queue.take(). In such a case, it is
          // safe to ignore the exception if the thread is being shutdown. We raise the exception
          // here again, because, it is ignored by ShutdownableThread if it is shutting down.
          throw ie
        case cacheUpdateException: FeatureCacheUpdateException =>
          error("Failed to process feature ZK node change event. The broker will eventually exit.", cacheUpdateException)
          throw new FatalExitError(1)
        case e: Exception =>
          // do not exit for exceptions unrelated to cache change processing (e.g. ZK session expiration)
          warn("Unexpected exception in feature ZK node change event processing; will continue processing.", e)
      }
    }
  }

  // Feature ZK node change handler.
  private object FeatureZNodeChangeHandler extends ZNodeChangeHandler {
    override val path: String = FeatureZNode.path

    override def handleCreation(): Unit = {
      info(s"Feature ZK node created at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDataChange(): Unit = {
      info(s"Feature ZK node updated at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDeletion(): Unit = {
      warn(s"Feature ZK node deleted at path: $path")
      // This event may happen, rarely (ex: ZK corruption or operational error).
      // In such a case, we prefer to just log a warning and treat the case as if the node is absent,
      // and populate the FinalizedFeatureCache with empty finalized features.
      queue.add(new FeatureCacheUpdater(path))
    }
  }

  object ZkStateChangeHandler extends StateChangeHandler {
    val path: String = FeatureZNode.path

    override val name: String = path

    override def afterInitializingSession(): Unit = {
      queue.add(new FeatureCacheUpdater(path))
    }
  }

  private val queue = new LinkedBlockingQueue[FeatureCacheUpdater]

  private val thread = new ChangeNotificationProcessorThread("feature-zk-node-event-process-thread")

  /**
   * This method initializes the feature ZK node change listener. Optionally, it also ensures to
   * update the FinalizedFeatureCache once with the latest contents of the feature ZK node
   * (if the node exists). This step helps ensure that feature incompatibilities (if any) in brokers
   * are conveniently detected before the initOrThrow() method returns to the caller. If feature
   * incompatibilities are detected, this method will throw an Exception to the caller, and the Broker
   * will exit eventually.
   *
   * @param waitOnceForCacheUpdateMs   # of milli seconds to wait for feature cache to be updated once.
   *                                   (should be > 0)
   *
   * @throws Exception if feature incompatibility check could not be finished in a timely manner
   */
  def initOrThrow(waitOnceForCacheUpdateMs: Long): Unit = {
    if (waitOnceForCacheUpdateMs <= 0) {
      throw new IllegalArgumentException(
        s"Expected waitOnceForCacheUpdateMs > 0, but provided: $waitOnceForCacheUpdateMs")
    }

    thread.start()
    zkClient.registerStateChangeHandler(ZkStateChangeHandler)
    zkClient.registerZNodeChangeHandlerAndCheckExistence(FeatureZNodeChangeHandler)
    val ensureCacheUpdateOnce = new FeatureCacheUpdater(
      FeatureZNodeChangeHandler.path, Some(new CountDownLatch(1)))
    queue.add(ensureCacheUpdateOnce)
    try {
      ensureCacheUpdateOnce.awaitUpdateOrThrow(waitOnceForCacheUpdateMs)
    } catch {
      case e: Exception => {
        close()
        throw e
      }
    }
  }

  /**
   * Closes the feature ZK node change listener by unregistering the listener from ZK client,
   * clearing the queue and shutting down the ChangeNotificationProcessorThread.
   */
  def close(): Unit = {
    zkClient.unregisterStateChangeHandler(ZkStateChangeHandler.name)
    zkClient.unregisterZNodeChangeHandler(FeatureZNodeChangeHandler.path)
    queue.clear()
    thread.shutdown()
  }

  // For testing only.
  def isListenerInitiated: Boolean = {
    thread.isRunning && thread.isAlive
  }

  // For testing only.
  def isListenerDead: Boolean = {
    !thread.isRunning && !thread.isAlive
  }
}
