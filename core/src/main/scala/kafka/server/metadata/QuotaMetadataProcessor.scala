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
import kafka.server.ConfigEntityName
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Sanitizer

import java.net.{InetAddress, UnknownHostException}
import scala.collection.mutable

/**
 * Watch for changes to quotas in the metadata log and update quota managers as necessary
 *
 * @param quotaManagers
 * @param connectionQuotas
 */
class QuotaMetadataProcessor(val quotaManagers: QuotaManagers,
                             val connectionQuotas: ConnectionQuotas,
                             val quotaCache: QuotaCache) extends BrokerMetadataProcessor with Logging {

  override def process(event: BrokerMetadataEvent): Unit = {
    event match {
      case MetadataLogEvent(apiMessages, _) =>
        apiMessages.forEach {
          case record: QuotaRecord => handleQuotaRecord(record)
          case _ => // Only care about quota records
        }
      case _ => // Only care about metadata events
    }
  }

  private[server] def handleQuotaRecord(quotaRecord: QuotaRecord): Unit = {
    val entityMap = mutable.Map[String, String]()
    quotaRecord.entity().forEach { entityData =>
      entityMap.put(entityData.entityType(), entityData.entityName())
    }

    if (entityMap.contains(ClientQuotaEntity.IP)) {
      // In the IP quota manager, None is used for default entity
      handleIpQuota(Option(entityMap(ClientQuotaEntity.IP)), quotaRecord)
      return
    }

    if (entityMap.contains(ClientQuotaEntity.USER) || entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
      // Need to handle null values for default entity name, so use "getOrElse" combined with "contains" checks
      val userVal = entityMap.getOrElse(ClientQuotaEntity.USER, null)
      val clientIdVal = entityMap.getOrElse(ClientQuotaEntity.CLIENT_ID, null)

      // In User+Client quota managers, "<default>" is used for default entity, so we need to represent all possible
      // combinations of values, defaults, and absent entities
      val userClientEntity = if (entityMap.contains(ClientQuotaEntity.USER) && entityMap.contains(ClientQuotaEntity.CLIENT_ID)) {
        if (userVal == null && clientIdVal == null) {
          DefaultUserDefaultClientIdEntity
        } else if (userVal == null) {
          DefaultUserClientIdEntity(clientIdVal)
        } else if (clientIdVal == null) {
          UserDefaultClientIdEntity(userVal)
        } else {
          UserClientIdEntity(userVal, clientIdVal)
        }
      } else if (entityMap.contains(ClientQuotaEntity.USER)) {
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
      handleUserClientQuota(
        userClientEntity,
        quotaRecord
      )
    } else {
      warn(s"Ignoring unsupported quota entity ${quotaRecord.entity()}")
    }
  }

  private[server] def handleIpQuota(ipName: Option[String], quotaRecord: QuotaRecord): Unit = {
    val inetAddress = try {
      ipName.map(InetAddress.getByName)
    } catch {
      case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ipName")
    }

    // Map to an appropriate entity
    val quotaEntity = if (ipName.isDefined) {
      IpEntity(ipName.get)
    } else {
      DefaultIpEntity
    }

    // The connection quota only understands the connection rate limit
    if (quotaRecord.key() != QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG) {
      warn(s"Ignoring unexpected quota key ${quotaRecord.key()} for entity $quotaEntity")
      return
    }

    // Update the cache
    quotaCache.updateQuotaCache(quotaEntity, quotaRecord)

    // Convert the value to an appropriate Option for the quota manager
    val newValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(quotaRecord.value).map(_.toInt)
    }
    connectionQuotas.updateIpConnectionRateQuota(inetAddress, newValue)
  }

  private[server] def handleUserClientQuota(quotaEntity: QuotaEntity, quotaRecord: QuotaRecord): Unit = {
    val managerOpt = quotaRecord.key() match {
      case QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.fetch)
      case QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => Some(quotaManagers.produce)
      case QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => Some(quotaManagers.request)
      case QuotaConfigs.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => Some(quotaManagers.controllerMutation)
      case _ => warn(s"Ignoring unexpected quota key ${quotaRecord.key()} for entity $quotaEntity"); None
    }

    if (managerOpt.isEmpty) {
      return
    }

    quotaCache.updateQuotaCache(quotaEntity, quotaRecord)

    // Convert entity into Options with sanitized values for QuotaManagers
    val (sanitizedUser, sanitizedClientId) = quotaEntity match {
      case UserEntity(user) => (Some(Sanitizer.sanitize(user)), None)
      case DefaultUserEntity => (Some(ConfigEntityName.Default), None)
      case ClientIdEntity(clientId) => (None, Some(Sanitizer.sanitize(clientId)))
      case DefaultClientIdEntity => (None, Some(ConfigEntityName.Default))
      case UserClientIdEntity(user, clientId) => (Some(Sanitizer.sanitize(user)), Some(Sanitizer.sanitize(clientId)))
      case UserDefaultClientIdEntity(user) => (Some(Sanitizer.sanitize(user)), Some(ConfigEntityName.Default))
      case DefaultUserClientIdEntity(clientId) => (Some(ConfigEntityName.Default), Some(Sanitizer.sanitize(clientId)))
      case DefaultUserDefaultClientIdEntity => (Some(ConfigEntityName.Default), Some(ConfigEntityName.Default))
      case IpEntity(_) | DefaultIpEntity => (None, None) // TODO should not get here, maybe throw?
    }

    val quotaValue = if (quotaRecord.remove()) {
      None
    } else {
      Some(new Quota(quotaRecord.value(), true))
    }

    managerOpt.foreach {
      manager => manager.updateQuota(
        sanitizedUser = sanitizedUser,
        clientId = sanitizedClientId.map(Sanitizer.desanitize),
        sanitizedClientId = sanitizedClientId,
        quota = quotaValue)
    }
  }
}
