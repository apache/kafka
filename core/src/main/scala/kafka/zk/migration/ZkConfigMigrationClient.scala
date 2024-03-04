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

package kafka.zk.migration

import kafka.server.{DynamicBrokerConfig, DynamicConfig, ZkAdminManager}
import kafka.utils.Logging
import kafka.zk.ZkMigrationClient.{logAndRethrow, wrapZkException}
import kafka.zk._
import kafka.zookeeper.{CreateRequest, DeleteRequest, SetDataRequest}
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigDef, ConfigResource}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.metadata.migration.ConfigMigrationClient.ClientQuotaVisitor
import org.apache.kafka.metadata.migration.{ConfigMigrationClient, MigrationClientException, ZkMigrationLeadershipState}
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.config.{ConfigEntityName, ConfigType}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}


import java.{lang, util}
import java.util.Properties
import java.util.function.{BiConsumer, Consumer}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ZkConfigMigrationClient(
  zkClient: KafkaZkClient,
  passwordEncoder: PasswordEncoder
) extends ConfigMigrationClient with Logging {

  val adminZkClient = new AdminZkClient(zkClient)


  /**
   * In ZK, we use the special string "&lt;default&gt;" to represent the default entity.
   * In KRaft, we use an empty string. This method builds an EntityData that converts the special ZK string
   * to the special KRaft string.
   */
  private def fromZkEntityName(entityName: String): String = {
    if (entityName.equals(ConfigEntityName.DEFAULT)) {
      ""
    } else {
      entityName
    }
  }

  private def toZkEntityName(entityName: String): String = {
    if (entityName.isEmpty) {
      ConfigEntityName.DEFAULT
    } else {
      entityName
    }
  }

  private def buildEntityData(entityType: String, entityName: String): EntityData = {
    new EntityData().setEntityType(entityType).setEntityName(fromZkEntityName(entityName))
  }


  override def iterateClientQuotas(visitor: ClientQuotaVisitor): Unit = {
    def migrateEntityType(zkEntityType: String, entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(zkEntityType).foreach { case (name, props) =>
        val entity = List(buildEntityData(entityType, name)).asJava

        ScramMechanism.values().filter(_ != ScramMechanism.UNKNOWN).foreach { mechanism =>
          val propertyValue = props.getProperty(mechanism.mechanismName)
          if (propertyValue != null) {
            val scramCredentials = ScramCredentialUtils.credentialFromString(propertyValue)
            logAndRethrow(this, s"Error in client quota visitor for SCRAM credential. User was $entity.") {
              visitor.visitScramCredential(name, mechanism, scramCredentials)
            }
            props.remove(mechanism.mechanismName)
          }
        }

        val quotaMap = ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).map {
          case (key, value) => key -> lang.Double.valueOf(value)
        }.toMap.asJava

        if (!quotaMap.isEmpty) {
          logAndRethrow(this, s"Error in client quota visitor. Entity was $entity.") {
            visitor.visitClientQuota(entity, quotaMap)
          }
        }
      }
    }

    migrateEntityType(ConfigType.USER, ClientQuotaEntity.USER)
    migrateEntityType(ConfigType.CLIENT, ClientQuotaEntity.CLIENT_ID)

    adminZkClient.fetchAllChildEntityConfigs(ConfigType.USER, ConfigType.CLIENT).foreach { case (name, props) =>
      // Taken from ZkAdminManager
      val components = name.split("/")
      if (components.size != 3 || components(1) != "clients")
        throw new IllegalArgumentException(s"Unexpected config path: $name")
      val entity = List(
        buildEntityData(ClientQuotaEntity.USER, components(0)),
        buildEntityData(ClientQuotaEntity.CLIENT_ID, components(2))
      )
      val quotaMap = props.asScala.map { case (key, value) =>
        val doubleValue = try lang.Double.valueOf(value) catch {
          case _: NumberFormatException =>
            throw new IllegalStateException(s"Unexpected client quota configuration value: $key -> $value")
        }
        key -> doubleValue
      }.asJava
      logAndRethrow(this, s"Error in client quota entity visitor. Entity was $entity.") {
        visitor.visitClientQuota(entity.asJava, quotaMap)
      }
    }

    migrateEntityType(ConfigType.IP, ClientQuotaEntity.IP)
  }

  override def iterateBrokerConfigs(configConsumer: BiConsumer[String, util.Map[String, String]]): Unit = {
    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.BROKER)
    zkClient.getEntitiesConfigs(ConfigType.BROKER, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = fromZkEntityName(broker)
      val decodedProps = props.asScala.map { case (key, value) =>
        if (DynamicBrokerConfig.isPasswordConfig(key))
          key -> passwordEncoder.decode(value).value
        else
          key -> value
      }.toMap.asJava

      logAndRethrow(this, s"Error in broker config consumer. Broker was $brokerResource.") {
        configConsumer.accept(brokerResource, decodedProps)
      }
    }
  }

  override def iterateTopicConfigs(configConsumer: BiConsumer[String, util.Map[String, String]]): Unit = {
    val topicEntities = zkClient.getAllEntitiesWithConfig(ConfigType.TOPIC)
    topicEntities.foreach { topic =>
      readTopicConfigs(topic, props => configConsumer.accept(topic, props))
    }
  }

  override def readTopicConfigs(topicName: String, configConsumer: Consumer[util.Map[String, String]]): Unit = {
    val topicResource = fromZkEntityName(topicName)
    val props = zkClient.getEntityConfigs(ConfigType.TOPIC, topicResource)
    val decodedProps = props.asScala.map { case (key, value) =>
      if (DynamicBrokerConfig.isPasswordConfig(key))
        key -> passwordEncoder.decode(value).value
      else
        key -> value
    }.toMap.asJava

    logAndRethrow(this, s"Error in topic config consumer. Topic was $topicResource.") {
      configConsumer.accept(decodedProps)
    }
  }

  override def writeConfigs(
    configResource: ConfigResource,
    configMap: util.Map[String, String],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val configType = configResource.`type`() match {
      case ConfigResource.Type.BROKER => Some(ConfigType.BROKER)
      case ConfigResource.Type.TOPIC => Some(ConfigType.TOPIC)
      case _ => None
    }

    val configName = toZkEntityName(configResource.name())
    if (configType.isDefined) {
      val props = new Properties()
      configMap.forEach { case (key, value) =>
        if (DynamicBrokerConfig.isPasswordConfig(key)) {
          props.put(key, passwordEncoder.encode(new Password(value)))
        } else
          props.put(key, value)
      }
      tryWriteEntityConfig(configType.get, configName, props, create = false, state) match {
        case Some(newState) =>
          newState
        case None =>
          val createPath = ConfigEntityTypeZNode.path(configType.get)
          debug(s"Recursively creating ZNode $createPath and attempting to write $configResource configs a second time.")
          zkClient.createRecursive(createPath, throwIfPathExists = false)

          tryWriteEntityConfig(configType.get, configName, props, create = true, state) match {
            case Some(newStateSecondTry) => newStateSecondTry
            case None => throw new MigrationClientException(
              s"Could not write ${configType.get} configs on second attempt when using Create instead of SetData.")
          }
      }
    } else {
      error(s"Not updating ZK for $configResource since it is not a Broker or Topic entity.")
      state
    }
  }

  override def deleteConfigs(
    configResource: ConfigResource,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val configType = configResource.`type`() match {
      case ConfigResource.Type.BROKER => Some(ConfigType.BROKER)
      case ConfigResource.Type.TOPIC => Some(ConfigType.TOPIC)
      case _ => None
    }

    val configName = toZkEntityName(configResource.name())
    if (configType.isDefined) {
      val path = ConfigEntityZNode.path(configType.get, configName)
      val requests = Seq(DeleteRequest(path, ZkVersion.MatchAnyVersion))
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)

      if (responses.head.resultCode.equals(Code.NONODE)) {
        // Not fatal.
        error(s"Did not delete $configResource since the node did not exist.")
        state
      } else if (responses.head.resultCode.equals(Code.OK)) {
        // Write the notification znode if our update was successful
        zkClient.createConfigChangeNotification(s"$configType/$configName")
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw KeeperException.create(responses.head.resultCode, path)
      }
    } else {
      error(s"Not updating ZK for $configResource since it is not a Broker or Topic entity.")
      state
    }
  }

  override def writeClientQuotas(
    entity: util.Map[String, String],
    quotas: util.Map[String, java.lang.Double],
    scram: util.Map[String, String],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val entityMap = entity.asScala
    val user = entityMap.get(ClientQuotaEntity.USER).map(toZkEntityName)
    val client = entityMap.get(ClientQuotaEntity.CLIENT_ID).map(toZkEntityName)
    val ip = entityMap.get(ClientQuotaEntity.IP).map(toZkEntityName)
    val props = new Properties()

    val (configType, path, configKeys) = if (user.isDefined && client.isEmpty) {
      (Some(ConfigType.USER), user, DynamicConfig.User.configKeys)
    } else if (user.isDefined && client.isDefined) {
      (Some(ConfigType.USER), Some(s"${user.get}/clients/${client.get}"),
        DynamicConfig.User.configKeys)
    } else if (client.isDefined) {
      (Some(ConfigType.CLIENT), client, DynamicConfig.Client.configKeys)
    } else if (ip.isDefined) {
      (Some(ConfigType.IP), ip, DynamicConfig.Ip.configKeys)
    } else {
      (None, None, Map.empty.asJava)
    }

    if (path.isEmpty) {
      error(s"Skipping unknown client quota entity $entity")
      return state
    }

    // This logic is duplicated from ZkAdminManager
    quotas.forEach { case (key, value) =>
      val configKey = configKeys.get(key)
      if (configKey == null) {
        throw new MigrationClientException(s"Invalid configuration key $key")
      } else {
        configKey.`type` match {
          case ConfigDef.Type.DOUBLE =>
            props.setProperty(key, value.toString)
          case ConfigDef.Type.LONG | ConfigDef.Type.INT =>
            val epsilon = 1e-6
            val intValue = if (configKey.`type` == ConfigDef.Type.LONG)
              (value + epsilon).toLong
            else
              (value + epsilon).toInt
            if ((intValue.toDouble - value).abs > epsilon)
              throw new InvalidRequestException(s"Configuration $key must be a ${configKey.`type`} value")
            props.setProperty(key, intValue.toString)
          case _ =>
            throw new MigrationClientException(s"Unexpected config type ${configKey.`type`}")
        }
      }
    }
    scram.forEach { case (key, value) => props.put(key, value) }

    // Try to write the client quota configs once with create=false, and again with create=true if the first operation fails
    tryWriteEntityConfig(configType.get, path.get, props, create = false, state) match {
      case Some(newState) =>
        newState
      case None =>
        // If we didn't update the migration state, we failed to write the client quota. Try again
        // after recursively create its parent znodes
        val createPath = if (user.isDefined && client.isDefined) {
          s"${ConfigEntityTypeZNode.path(configType.get)}/${user.get}/clients"
        } else {
          ConfigEntityTypeZNode.path(configType.get)
        }
        zkClient.createRecursive(createPath, throwIfPathExists = false)
        debug(s"Recursively creating ZNode $createPath and attempting to write $entity quotas a second time.")

        tryWriteEntityConfig(configType.get, path.get, props, create = true, state) match {
          case Some(newStateSecondTry) => newStateSecondTry
          case None => throw new MigrationClientException(
            s"Could not write client quotas for $entity on second attempt when using Create instead of SetData")
        }
    }
  }

  // Try to update an entity config and the migration state. If NoNode is encountered, it probably means we
  // need to recursively create the parent ZNode. In this case, return None.
  private def tryWriteEntityConfig(
    entityType: String,
    path: String,
    props: Properties,
    create: Boolean,
    state: ZkMigrationLeadershipState
  ): Option[ZkMigrationLeadershipState] = wrapZkException {
    val configData = ConfigEntityZNode.encode(props)
    val requests = if (create) {
      Seq(CreateRequest(ConfigEntityZNode.path(entityType, path), configData, zkClient.defaultAcls(path), CreateMode.PERSISTENT))
    } else {
      Seq(SetDataRequest(ConfigEntityZNode.path(entityType, path), configData, ZkVersion.MatchAnyVersion))
    }
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    if (!create && responses.head.resultCode.equals(Code.NONODE)) {
      // Not fatal. Just means we need to Create this node instead of SetData
      None
    } else if (responses.head.resultCode.equals(Code.OK)) {
      // Write the notification znode if our update was successful
      zkClient.createConfigChangeNotification(s"$entityType/$path")
      Some(state.withMigrationZkVersion(migrationZkVersion))
    } else {
      throw KeeperException.create(responses.head.resultCode, path)
    }
  }
}

