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

import java.nio.charset.StandardCharsets

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.utils.{Json, Logging}
import kafka.utils.json.JsonObject
import kafka.zk.{AdminZkClient, ConfigEntityChangeNotificationSequenceZNode, ConfigEntityChangeNotificationZNode, KafkaZkClient}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.security.scram.internal.ScramMechanism
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Represents all the entities that can be configured via ZK
 */
object ConfigType {
  val Topic = "topics"
  val Client = "clients"
  val User = "users"
  val Broker = "brokers"
  val all = Seq(Topic, Client, User, Broker)
}

object ConfigEntityName {
  val Default = "<default>"
}

/**
 * This class initiates and carries out config changes for all entities defined in ConfigType.
 *
 * It works as follows.
 *
 * Config is stored under the path: /config/entityType/entityName
 *   E.g. /config/topics/<topic_name> and /config/clients/<clientId>
 * This znode stores the overrides for this entity in properties format with defaults stored using entityName "<default>".
 * Multiple entity names may be specified (eg. <user, client-id> quotas) using a hierarchical path:
 *   E.g. /config/users/<user>/clients/<clientId>
 *
 * To avoid watching all topics for changes instead we have a notification path
 *   /config/changes
 * The DynamicConfigManager has a child watch on this path.
 *
 * To update a config we first update the config properties. Then we create a new sequential
 * znode under the change path which contains the name of the entityType and entityName that was updated, say
 *   /config/changes/config_change_13321
 * The sequential znode contains data in this format: {"version" : 1, "entity_type":"topic/client", "entity_name" : "topic_name/client_id"}
 * This is just a notification--the actual config change is stored only once under the /config/entityType/entityName path.
 * Version 2 of notifications has the format: {"version" : 2, "entity_path":"entity_type/entity_name"}
 * Multiple entities may be specified as a hierarchical path (eg. users/<user>/clients/<clientId>).
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
class DynamicConfigManager(private val zkClient: KafkaZkClient,
                           private val configHandlers: Map[String, ConfigHandler],
                           private val changeExpirationMs: Long = 15*60*1000,
                           private val time: Time = Time.SYSTEM) extends Logging {
  val adminZkClient = new AdminZkClient(zkClient)

  object ConfigChangedNotificationHandler extends NotificationHandler {
    override def processNotification(jsonBytes: Array[Byte]) = {
      // Ignore non-json notifications because they can be from the deprecated TopicConfigManager
      Json.parseBytes(jsonBytes).foreach { js =>
        val jsObject = js.asJsonObjectOption.getOrElse {
          throw new IllegalArgumentException("Config change notification has an unexpected value. The format is:" +
            """{"version" : 1, "entity_type":"topics/clients", "entity_name" : "topic_name/client_id"} or """ +
            """{"version" : 2, "entity_path":"entity_type/entity_name"}. """ +
            s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
        }
        jsObject("version").to[Int] match {
          case 1 => processEntityConfigChangeVersion1(jsonBytes, jsObject)
          case 2 => processEntityConfigChangeVersion2(jsonBytes, jsObject)
          case version => throw new IllegalArgumentException("Config change notification has unsupported version " +
            s"'$version', supported versions are 1 and 2.")
        }
      }
    }

    private def processEntityConfigChangeVersion1(jsonBytes: Array[Byte], js: JsonObject) {
      val validConfigTypes = Set(ConfigType.Topic, ConfigType.Client)
      val entityType = js.get("entity_type").flatMap(_.to[Option[String]]).filter(validConfigTypes).getOrElse {
        throw new IllegalArgumentException("Version 1 config change notification must have 'entity_type' set to " +
          s"'clients' or 'topics'. Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val entity = js.get("entity_name").flatMap(_.to[Option[String]]).getOrElse {
        throw new IllegalArgumentException("Version 1 config change notification does not specify 'entity_name'. " +
          s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val entityConfig = adminZkClient.fetchEntityConfig(entityType, entity)
      info(s"Processing override for entityType: $entityType, entity: $entity with config: $entityConfig")
      configHandlers(entityType).processConfigChanges(entity, entityConfig)

    }

    private def processEntityConfigChangeVersion2(jsonBytes: Array[Byte], js: JsonObject) {

      val entityPath = js.get("entity_path").flatMap(_.to[Option[String]]).getOrElse {
        throw new IllegalArgumentException(s"Version 2 config change notification must specify 'entity_path'. " +
          s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val index = entityPath.indexOf('/')
      val rootEntityType = entityPath.substring(0, index)
      if (index < 0 || !configHandlers.contains(rootEntityType)) {
        val entityTypes = configHandlers.keys.map(entityType => s"'$entityType'/").mkString(", ")
        throw new IllegalArgumentException("Version 2 config change notification must have 'entity_path' starting with " +
          s"one of $entityTypes. Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }
      val fullSanitizedEntityName = entityPath.substring(index + 1)

      val entityConfig = adminZkClient.fetchEntityConfig(rootEntityType, fullSanitizedEntityName)
      val loggableConfig = entityConfig.asScala.map {
        case (k, v) => (k, if (ScramMechanism.isScram(k)) Password.HIDDEN else v)
      }
      info(s"Processing override for entityPath: $entityPath with config: $loggableConfig")
      configHandlers(rootEntityType).processConfigChanges(fullSanitizedEntityName, entityConfig)

    }
  }

  private val configChangeListener = new ZkNodeChangeNotificationListener(zkClient, ConfigEntityChangeNotificationZNode.path,
    ConfigEntityChangeNotificationSequenceZNode.SequenceNumberPrefix, ConfigChangedNotificationHandler)

  /**
   * Begin watching for config changes
   */
  def startup(): Unit = {
    configChangeListener.init()

    // Apply all existing client/user configs to the ClientIdConfigHandler/UserConfigHandler to bootstrap the overrides
    configHandlers.foreach {
      case (ConfigType.User, handler) =>
        adminZkClient.fetchAllEntityConfigs(ConfigType.User).foreach {
          case (sanitizedUser, properties) => handler.processConfigChanges(sanitizedUser, properties)
        }
        adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach {
          case (sanitizedUserClientId, properties) => handler.processConfigChanges(sanitizedUserClientId, properties)
        }
      case (configType, handler) =>
        adminZkClient.fetchAllEntityConfigs(configType).foreach {
          case (entityName, properties) => handler.processConfigChanges(entityName, properties)
        }
    }
  }

  def shutdown(): Unit = {
    configChangeListener.close()
  }
}
