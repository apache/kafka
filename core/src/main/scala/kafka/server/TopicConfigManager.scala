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

import java.util.Properties
import scala.collection._
import kafka.log._
import kafka.utils._
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}

/**
 * This class initiates and carries out topic config changes.
 * 
 * It works as follows.
 * 
 * Config is stored under the path
 *   /brokers/topics/<topic_name>/config
 * This znode stores the topic-overrides for this topic (but no defaults) in properties format.
 * 
 * To avoid watching all topics for changes instead we have a notification path
 *   /brokers/config_changes
 * The TopicConfigManager has a child watch on this path.
 * 
 * To update a topic config we first update the topic config properties. Then we create a new sequential
 * znode under the change path which contains the name of the topic that was updated, say
 *   /brokers/config_changes/config_change_13321
 * This is just a notification--the actual config change is stored only once under the /brokers/topics/<topic_name>/config path.
 *   
 * This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications.
 * It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds
 * it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification. 
 * For any new changes it reads the new configuration, combines it with the defaults, and updates the log config 
 * for all logs for that topic (if any) that it has.
 * 
 * Note that config is always read from the config path in zk, the notification is just a trigger to do so. So if a broker is
 * down and misses a change that is fine--when it restarts it will be loading the full config anyway. Note also that
 * if there are two consecutive config changes it is possible that only the last one will be applied (since by the time the 
 * broker reads the config the both changes may have been made). In this case the broker would needlessly refresh the config twice,
 * but that is harmless.
 * 
 * On restart the config manager re-processes all notifications. This will usually be wasted work, but avoids any race conditions
 * on startup where a change might be missed between the initial config load and registering for change notifications.
 * 
 */
class TopicConfigManager(private val zkClient: ZkClient,
                         private val logManager: LogManager,
                         private val changeExpirationMs: Long = 15*60*1000,
                         private val time: Time = SystemTime) extends Logging {
  private var lastExecutedChange = -1L
  
  /**
   * Begin watching for config changes
   */
  def startup() {
    ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.TopicConfigChangesPath)
    zkClient.subscribeChildChanges(ZkUtils.TopicConfigChangesPath, ConfigChangeListener)
    processAllConfigChanges()
  }
  
  /**
   * Process all config changes
   */
  private def processAllConfigChanges() {
    val configChanges = zkClient.getChildren(ZkUtils.TopicConfigChangesPath)
    import JavaConversions._
    processConfigChanges((configChanges: mutable.Buffer[String]).sorted)
  }

  /**
   * Process the given list of config changes
   */
  private def processConfigChanges(notifications: Seq[String]) {
    if (notifications.size > 0) {
      info("Processing config change notification(s)...")
      val now = time.milliseconds
      val logs = logManager.logsByTopicPartition.toBuffer
      val logsByTopic = logs.groupBy(_._1.topic).mapValues(_.map(_._2))
      for (notification <- notifications) {
        val changeId = changeNumber(notification)
        if (changeId > lastExecutedChange) {
          val changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification
          val (jsonOpt, stat) = ZkUtils.readDataMaybeNull(zkClient, changeZnode)
          if(jsonOpt.isDefined) {
            val json = jsonOpt.get
            val topic = json.substring(1, json.length - 1) // hacky way to dequote
            if (logsByTopic.contains(topic)) {
              /* combine the default properties with the overrides in zk to create the new LogConfig */
              val props = new Properties(logManager.defaultConfig.toProps)
              props.putAll(AdminUtils.fetchTopicConfig(zkClient, topic))
              val logConfig = LogConfig.fromProps(props)
              for (log <- logsByTopic(topic))
                log.config = logConfig
              info("Processed topic config change %d for topic %s, setting new config to %s.".format(changeId, topic, props))
              purgeObsoleteNotifications(now, notifications)
            }
          }
          lastExecutedChange = changeId
        }
      }
    }
  }
  
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]) {
    for(notification <- notifications.sorted) {
      val (jsonOpt, stat) = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.TopicConfigChangesPath + "/" + notification)
      if(jsonOpt.isDefined) {
        val changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification
        if (now - stat.getCtime > changeExpirationMs) {
          debug("Purging config change notification " + notification)
          ZkUtils.deletePath(zkClient, changeZnode)
        } else {
          return
        }
      }
    }
  }
    
  /* get the change number from a change notification znode */
  private def changeNumber(name: String): Long = name.substring(AdminUtils.TopicConfigChangeZnodePrefix.length).toLong
  
  /**
   * A listener that applies config changes to logs
   */
  object ConfigChangeListener extends IZkChildListener {
    override def handleChildChange(path: String, chillins: java.util.List[String]) {
      try {
        import JavaConversions._
        processConfigChanges(chillins: mutable.Buffer[String])
      } catch {
        case e: Exception => error("Error processing config change:", e)
      }
    }
  }

}