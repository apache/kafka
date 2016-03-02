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

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.security.auth.Resource
import kafka.utils.Json
import kafka.utils.Logging
import kafka.utils.SystemTime
import kafka.utils.Time
import kafka.utils.ZkUtils
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._
import kafka.admin.AdminUtils
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}


/**
 * Represents all the entities that can be configured via ZK
 */
object ConfigType {
  val Topic = "topics"
  val Client = "clients"
}

/**
 * This class initiates and carries out config changes for all entities defined in ConfigType.
 *
 * It works as follows.
 *
 * Config is stored under the path: /config/entityType/entityName
 *   E.g. /config/topics/<topic_name> and /config/clients/<clientId>
 * This znode stores the overrides for this entity (but no defaults) in properties format.
 *
 * To avoid watching all topics for changes instead we have a notification path
 *   /config/changes
 * The DynamicConfigManager has a child watch on this path.
 *
 * To update a config we first update the config properties. Then we create a new sequential
 * znode under the change path which contains the name of the entityType and entityName that was updated, say
 *   /config/changes/config_change_13321
 * The sequential znode contains data in this format: {"version" : 1, "entityType":"topic/client", "entityName" : "topic_name/client_id"}
 * This is just a notification--the actual config change is stored only once under the /config/entityType/entityName path.
 *
 * This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications.
 * It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds
 * it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification.
 * For any new changes it reads the new configuration, combines it with the defaults, and updates the existing config.
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
class DynamicConfigManager(private val zkUtils: ZkUtils,
                           private val configHandlers: Map[String, ConfigHandler],
                           private val changeExpirationMs: Long = 15*60*1000,
                           private val time: Time = SystemTime) extends Logging {
  private var lastExecutedChange = -1L

  object ConfigChangedNotificationHandler extends NotificationHandler {
    override def processNotification(json: String) = {
      Json.parseFull(json) match {
        case None => // There are no config overrides.
        // Ignore non-json notifications because they can be from the deprecated TopicConfigManager
        case Some(mapAnon: Map[_, _]) =>
          val map = mapAnon collect
            { case (k: String, v: Any) => k -> v }
          require(map("version") == 1)

          val entityType = map.get("entity_type") match {
            case Some(ConfigType.Topic) => ConfigType.Topic
            case Some(ConfigType.Client) => ConfigType.Client
            case _ => throw new IllegalArgumentException("Config change notification must have 'entity_type' set to either 'client' or 'topic'." +
              " Received: " + json)
          }

          val entity = map.get("entity_name") match {
            case Some(value: String) => value
            case _ => throw new IllegalArgumentException("Config change notification does not specify 'entity_name'. Received: " + json)
          }
          val entityConfig = AdminUtils.fetchEntityConfig(zkUtils, entityType, entity)
          logger.info(s"Processing override for entityType: $entityType, entity: $entity with config: $entityConfig")
          configHandlers(entityType).processConfigChanges(entity, entityConfig)

        case o => throw new IllegalArgumentException("Config change notification has an unexpected value. The format is:" +
          "{\"version\" : 1," +
          " \"entity_type\":\"topic/client\"," +
          " \"entity_name\" : \"topic_name/client_id\"}." +
          " Received: " + json)
      }
    }
  }

  private val configChangeListener = new ZkNodeChangeNotificationListener(zkUtils, ZkUtils.EntityConfigChangesPath, AdminUtils.EntityConfigChangeZnodePrefix, ConfigChangedNotificationHandler)

  /**
   * Begin watching for config changes
   */
  def startup(): Unit = {
    configChangeListener.init()
  }
}
