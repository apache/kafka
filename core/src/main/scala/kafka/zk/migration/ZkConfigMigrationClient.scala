package kafka.zk.migration

import kafka.server.{ConfigEntityName, ConfigType, DynamicBrokerConfig, ZkAdminManager}
import kafka.utils.{Logging, PasswordEncoder}
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk._
import kafka.zookeeper.{CreateRequest, SetDataRequest}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.metadata.migration.{ConfigMigrationClient, MigrationClientException, ZkMigrationLeadershipState}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}

import java.{lang, util}
import java.util.Properties
import java.util.function.BiConsumer
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ZkConfigMigrationClient(
  zkClient: KafkaZkClient,
  passwordEncoder: PasswordEncoder
) extends ConfigMigrationClient with Logging {

  val adminZkClient = new AdminZkClient(zkClient)

  override def iterateClientQuotas(
    quotaEntityConsumer: BiConsumer[util.List[EntityData], util.Map[String, lang.Double]]
  ): Unit = {
    def migrateEntityType(entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(entityType).foreach { case (name, props) =>
        val entity = List(new EntityData().setEntityType(entityType).setEntityName(name)).asJava
        val quotaMap = ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).map {
          case (key, value) => key -> lang.Double.valueOf(value)
        }.toMap.asJava
        quotaEntityConsumer.accept(entity, quotaMap)
      }
    }

    migrateEntityType(ConfigType.User)
    migrateEntityType(ConfigType.Client)

    adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach { case (name, props) =>
      // Taken from ZkAdminManager
      val components = name.split("/")
      if (components.size != 3 || components(1) != "clients")
        throw new IllegalArgumentException(s"Unexpected config path: ${name}")
      val entity = List(
        new EntityData().setEntityType(ConfigType.User).setEntityName(components(0)),
        new EntityData().setEntityType(ConfigType.Client).setEntityName(components(2))
      )
      val quotaMap = ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).asJava
      quotaEntityConsumer.accept(entity.asJava, quotaMap)
    }

    migrateEntityType(ConfigType.Ip)
  }

  override def iterateBrokerConfigs(configConsumer: BiConsumer[String, util.Map[String, String]]): Unit = {
    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.Broker)
    zkClient.getEntitiesConfigs(ConfigType.Broker, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = if (broker == ConfigEntityName.Default) {
        ""
      } else {
        broker
      }
      val decodedProps = props.asScala.map { case (key, value) =>
        if (DynamicBrokerConfig.isPasswordConfig(key))
          key -> passwordEncoder.decode(value).value
        else
          key -> value
      }.toMap.asJava

      configConsumer.accept(brokerResource, decodedProps)
    }
  }

  override def writeConfigs(
    configResource: ConfigResource,
    configMap: util.Map[String, String],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val configType = configResource.`type`() match {
      case ConfigResource.Type.BROKER => Some(ConfigType.Broker)
      case ConfigResource.Type.TOPIC => Some(ConfigType.Topic)
      case _ => None
    }

    val configName = configResource.name()
    if (configType.isDefined) {
      val props = new Properties()
      configMap.forEach { case (key, value) => props.put(key, value) }
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
      debug(s"Not updating ZK for $configResource since it is not a Broker or Topic entity.")
      state
    }
  }

  override def deleteConfigs(
    configResource: ConfigResource,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    // TODO
    throw new IllegalArgumentException()
  }

  override def writeClientQuotas(
    entity: util.Map[String, String],
    quotas: util.Map[String, java.lang.Double],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val entityMap = entity.asScala
    val hasUser = entityMap.contains(ClientQuotaEntity.USER)
    val hasClient = entityMap.contains(ClientQuotaEntity.CLIENT_ID)
    val hasIp = entityMap.contains(ClientQuotaEntity.IP)
    val props = new Properties()
    // We store client quota values as strings in the ZK JSON
    quotas.forEach { case (key, value) => props.put(key, value.toString) }
    val (configType, path) = if (hasUser && !hasClient) {
      (Some(ConfigType.User), Some(entityMap(ClientQuotaEntity.USER)))
    } else if (hasUser && hasClient) {
      (Some(ConfigType.User), Some(s"${entityMap(ClientQuotaEntity.USER)}/clients/${entityMap(ClientQuotaEntity.CLIENT_ID)}"))
    } else if (hasClient) {
      (Some(ConfigType.Client), Some(entityMap(ClientQuotaEntity.CLIENT_ID)))
    } else if (hasIp) {
      (Some(ConfigType.Ip), Some(entityMap(ClientQuotaEntity.IP)))
    } else {
      (None, None)
    }

    if (path.isEmpty) {
      error(s"Skipping unknown client quota entity $entity")
      return state
    }

    // Try to write the client quota configs once with create=false, and again with create=true if the first operation fails
    tryWriteEntityConfig(configType.get, path.get, props, create = false, state) match {
      case Some(newState) =>
        newState
      case None =>
        // If we didn't update the migration state, we failed to write the client quota. Try again
        // after recursively create its parent znodes
        val createPath = if (hasUser && hasClient) {
          s"${ConfigEntityTypeZNode.path(configType.get)}/${entityMap(ClientQuotaEntity.USER)}/clients"
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

