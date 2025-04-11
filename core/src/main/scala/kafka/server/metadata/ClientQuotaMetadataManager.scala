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

package kafka.server.metadata

import kafka.network.ConnectionQuotas
import kafka.server.ClientQuotaManager
import kafka.server.ClientQuotaManager.BaseUserEntity
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.ClientQuotaMetadataManager.transferToClientQuotaEntity
import kafka.utils.Logging
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Sanitizer
import org.apache.kafka.server.quota.ClientQuotaEntity.{ConfigEntity => ClientQuotaConfigEntity} 

import java.net.{InetAddress, UnknownHostException}
import org.apache.kafka.image.{ClientQuotaDelta, ClientQuotasDelta}
import org.apache.kafka.server.config.QuotaConfig

import scala.jdk.OptionConverters.RichOptionalDouble



// A strict hierarchy of entities that we support
sealed trait QuotaEntity
case class IpEntity(ip: String) extends QuotaEntity
case object DefaultIpEntity extends QuotaEntity
case class UserEntity(user: String) extends QuotaEntity
case object DefaultUserEntity extends QuotaEntity
case class ClientIdEntity(clientId: String) extends QuotaEntity
case object DefaultClientIdEntity extends QuotaEntity
case class ExplicitUserExplicitClientIdEntity(user: String, clientId: String) extends QuotaEntity
case class ExplicitUserDefaultClientIdEntity(user: String) extends QuotaEntity
case class DefaultUserExplicitClientIdEntity(clientId: String) extends QuotaEntity
case object DefaultUserDefaultClientIdEntity extends QuotaEntity

/**
 * Process quota metadata records as they appear in the metadata log and update quota managers and cache as necessary
 */
class ClientQuotaMetadataManager(private[metadata] val quotaManagers: QuotaManagers,
                                 private[metadata] val connectionQuotas: ConnectionQuotas) extends Logging {

  def update(quotasDelta: ClientQuotasDelta): Unit = {
    quotasDelta.changes().forEach { (key, value) =>
      update(key, value)
    }
  }

  private def update(entity: ClientQuotaEntity, quotaDelta: ClientQuotaDelta): Unit = {
    if (entity.entries().containsKey(ClientQuotaEntity.IP)) {
      // In the IP quota manager, None is used for default entity
      val ipEntity = Option(entity.entries().get(ClientQuotaEntity.IP)) match {
        case Some(ip) => IpEntity(ip)
        case None => DefaultIpEntity
      }
      handleIpQuota(ipEntity, quotaDelta)
    } else if (entity.entries().containsKey(ClientQuotaEntity.USER) ||
        entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
      // These values may be null, which is why we needed to use containsKey.
      val userVal = entity.entries().get(ClientQuotaEntity.USER)
      val clientIdVal = entity.entries().get(ClientQuotaEntity.CLIENT_ID)

      // In User+Client quota managers, "<default>" is used for default entity, so we need to represent all possible
      // combinations of values, defaults, and absent entities
      val userClientEntity = if (entity.entries().containsKey(ClientQuotaEntity.USER) &&
          entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
        if (userVal == null && clientIdVal == null) {
          DefaultUserDefaultClientIdEntity
        } else if (userVal == null) {
          DefaultUserExplicitClientIdEntity(clientIdVal)
        } else if (clientIdVal == null) {
          ExplicitUserDefaultClientIdEntity(userVal)
        } else {
          ExplicitUserExplicitClientIdEntity(userVal, clientIdVal)
        }
      } else if (entity.entries().containsKey(ClientQuotaEntity.USER)) {
        if (userVal == null) {
          DefaultUserEntity
        } else {
          UserEntity(userVal)
        }
      } else {
        if (clientIdVal == null) {
          DefaultClientIdEntity
        } else {
          ClientIdEntity(clientIdVal)
        }
      }
      quotaDelta.changes().forEach { (key, value) =>
        handleUserClientQuotaChange(userClientEntity, key, value.toScala)
      }
    } else {
      warn(s"Ignoring unsupported quota entity $entity.")
    }
  }

  private[metadata] def handleIpQuota(ipEntity: QuotaEntity, quotaDelta: ClientQuotaDelta): Unit = {
    val inetAddress = ipEntity match {
      case IpEntity(ip) =>
        try {
          Some(InetAddress.getByName(ip))
        } catch {
          case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ip")
        }
      case DefaultIpEntity => None
      case _ => throw new IllegalStateException("Should only handle IP quota entities here")
    }

    quotaDelta.changes().forEach { (key, value) =>
      // The connection quota only understands the connection rate limit
      val quotaName = key
      val quotaValue = value
      if (!quotaName.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
        warn(s"Ignoring unexpected quota key $quotaName for entity $ipEntity")
      } else {
        try {
          connectionQuotas.updateIpConnectionRateQuota(inetAddress, quotaValue.toScala.map(_.toInt))
        } catch {
          case t: Throwable => error(s"Failed to update IP quota $ipEntity", t)
        }
      }
    }
  }

  private def handleUserClientQuotaChange(quotaEntity: QuotaEntity, key: String, newValue: Option[Double]): Unit = {
    val manager = key match {
      case QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => quotaManagers.fetch
      case QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => quotaManagers.produce
      case QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => quotaManagers.request
      case QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => quotaManagers.controllerMutation
      case _ =>
        warn(s"Ignoring unexpected quota key $key for entity $quotaEntity")
        return
    }

    // Convert entity into Options with sanitized values for QuotaManagers
    val (userEntity, clientEntity) = transferToClientQuotaEntity(quotaEntity)

    val quotaValue = newValue.map(new Quota(_, true))
    try {
      manager.updateQuota(
        userEntity = userEntity,
        clientEntity = clientEntity,
        quota = quotaValue
      )
    } catch {
      case t: Throwable => error(s"Failed to update user-client quota $quotaEntity", t)
    }
  }
}

object ClientQuotaMetadataManager {

  def transferToClientQuotaEntity(quotaEntity: QuotaEntity): (Option[BaseUserEntity], Option[ClientQuotaConfigEntity]) = {
    quotaEntity match {
      case UserEntity(user) =>
        (Some(ClientQuotaManager.UserEntity(Sanitizer.sanitize(user))), None)
      case DefaultUserEntity =>
        (Some(ClientQuotaManager.DefaultUserEntity), None)
      case ClientIdEntity(clientId) =>
        (None, Some(ClientQuotaManager.ClientIdEntity(clientId)))
      case DefaultClientIdEntity =>
        (None, Some(ClientQuotaManager.DefaultClientIdEntity))
      case ExplicitUserExplicitClientIdEntity(user, clientId) =>
        (Some(ClientQuotaManager.UserEntity(Sanitizer.sanitize(user))), Some(ClientQuotaManager.ClientIdEntity(clientId)))
      case ExplicitUserDefaultClientIdEntity(user) =>
        (Some(ClientQuotaManager.UserEntity(Sanitizer.sanitize(user))), Some(ClientQuotaManager.DefaultClientIdEntity))
      case DefaultUserExplicitClientIdEntity(clientId) =>
        (Some(ClientQuotaManager.DefaultUserEntity), Some(ClientQuotaManager.ClientIdEntity(clientId)))
      case DefaultUserDefaultClientIdEntity =>
        (Some(ClientQuotaManager.DefaultUserEntity), Some(ClientQuotaManager.DefaultClientIdEntity))
      case IpEntity(_) | DefaultIpEntity => throw new IllegalStateException("Should not see IP quota entities here")
    }
  }
}