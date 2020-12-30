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

import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.metadata.QuotaRecord
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilter}

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable


// Types for primary quota cache
sealed trait QuotaEntity {
  val toMap: Map[String, String]
}
case class IpEntity(ip: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.IP -> ip)
}
case object DefaultIpEntity extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.IP -> null)
}

case class UserEntity(user: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> user)
}
case object DefaultUserEntity extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> null)
}

case class ClientIdEntity(clientId: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.CLIENT_ID -> clientId)
}
case object DefaultClientIdEntity extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.CLIENT_ID -> null)
}

case class UserClientIdEntity(user: String, clientId: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> user, ClientQuotaEntity.CLIENT_ID -> clientId)
}
case class UserDefaultClientIdEntity(user: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> user, ClientQuotaEntity.CLIENT_ID -> null)
}
case class DefaultUserClientIdEntity(clientId: String) extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> null, ClientQuotaEntity.CLIENT_ID -> clientId)
}
case object DefaultUserDefaultClientIdEntity extends QuotaEntity {
  val toMap = Map(ClientQuotaEntity.USER -> null, ClientQuotaEntity.CLIENT_ID -> null)
}


// A type for the cache index keys
sealed trait CacheIndexKey
case object DefaultUser extends CacheIndexKey
case class SpecificUser(user: String) extends CacheIndexKey
case object DefaultClientId extends CacheIndexKey
case class SpecificClientId(clientId: String) extends CacheIndexKey
case object DefaultIp extends CacheIndexKey
case class SpecificIp(ip: String) extends CacheIndexKey


// Different types of matching constraints
sealed trait QuotaMatch
case class ExactMatch(entityName: String) extends QuotaMatch
case object DefaultMatch extends QuotaMatch
case object TypeMatch extends QuotaMatch


class QuotaCache {
  type QuotaCacheIndex = mutable.HashMap[CacheIndexKey, mutable.HashSet[QuotaEntity]]

  // A cache of the quota entities and their current quota values
  private val quotaCache = new mutable.HashMap[QuotaEntity, mutable.Map[String, Double]]

  // An index of user or client to a set of corresponding cache entities. This is used for flexible lookups
  private val userEntityIndex = new QuotaCacheIndex
  private val clientIdEntityIndex = new QuotaCacheIndex
  private val ipEntityIndex = new QuotaCacheIndex

  private val lock = new ReentrantReadWriteLock()

  /**
   * Return quota entries for a given filter. These entries are returned from an in-memory cache and may not reflect
   * the latest state of the quotas according to the controller.
   *
   * @param quotaFilter       A quota entity filter
   * @return                  A mapping of quota entities along with their quota values
   */
  def describeClientQuotas(quotaFilter: ClientQuotaFilter): Map[QuotaEntity, Map[String, Double]] = inReadLock(lock) {

    // Do some preliminary validation of the filter types and convert them to correct QuotaMatch type
    val entityFilters: mutable.Map[String, QuotaMatch] = mutable.HashMap.empty
    quotaFilter.components().forEach(component => {
      val entityType = component.entityType()
      if (entityFilters.contains(entityType)) {
        throw new InvalidRequestException(s"Duplicate ${entityType} filter component entity type")
      } else if (entityType.isEmpty) {
        throw new InvalidRequestException("Unexpected empty filter component entity type")
      } else if (!ClientQuotaEntity.isValidEntityType(entityType)) {
        throw new UnsupportedVersionException(s"Custom entity type ${entityType} not supported")
      }

      // A present "match()" is an exact match on name, an absent "match()" is a match on the default entity,
      // and a null "match()" is a match on the entity type
      val entityMatch = if (component.`match`() != null && component.`match`().isPresent) {
        ExactMatch(component.`match`().get())
      } else if (component.`match`() != null) {
        DefaultMatch
      } else {
        TypeMatch
      }
      entityFilters.put(entityType, entityMatch)
    })

    if (entityFilters.isEmpty) {
      // TODO return empty or throw exception here?
      return Map.empty
    }

    // We do not allow IP filters to be combined with user or client filters
    if (entityFilters.contains(ClientQuotaEntity.IP) && entityFilters.size > 1) {
      throw new InvalidRequestException("Invalid entity filter component combination, IP filter component should " +
        "not be used with user or clientId filter component.")
    }

    val ipMatch = entityFilters.get(ClientQuotaEntity.IP)
    val ipIndexMatches: Set[QuotaEntity] = if (ipMatch.isDefined) {
      ipMatch.get match {
        case ExactMatch(ip) => ipEntityIndex.getOrElse(SpecificIp(ip), Set()).toSet
        case DefaultMatch => ipEntityIndex.getOrElse(DefaultIp, Set()).toSet
        case TypeMatch => ipEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val userMatch = entityFilters.get(ClientQuotaEntity.USER)
    val userIndexMatches: Set[QuotaEntity] = if (userMatch.isDefined) {
      userMatch.get match {
        case ExactMatch(user) => userEntityIndex.getOrElse(SpecificUser(user), Set()).toSet
        case DefaultMatch => userEntityIndex.getOrElse(DefaultUser, Set()).toSet
        case TypeMatch => userEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val clientMatch = entityFilters.get(ClientQuotaEntity.CLIENT_ID)
    val clientIndexMatches: Set[QuotaEntity] = if (clientMatch.isDefined) {
      clientMatch.get match {
        case ExactMatch(clientId) => clientIdEntityIndex.getOrElse(SpecificClientId(clientId), Set()).toSet
        case DefaultMatch => clientIdEntityIndex.getOrElse(DefaultClientId, Set()).toSet
        case TypeMatch => clientIdEntityIndex.values.flatten.toSet
      }
    } else {
      Set()
    }

    val candidateMatches: Set[QuotaEntity] = if (userMatch.isDefined && clientMatch.isDefined) {
      userIndexMatches.intersect(clientIndexMatches)
    } else if (userMatch.isDefined) {
      userIndexMatches
    } else if (clientMatch.isDefined) {
      clientIndexMatches
    } else if (ipMatch.isDefined) {
      ipIndexMatches
    } else {
      // TODO Should not get here, maybe throw?
      Set()
    }

    val filteredMatches: Set[QuotaEntity] = if (quotaFilter.strict()) {
      // If in strict mode, need to remove any matches with extra entity types. This only applies to results with
      // both user and clientId parts
      candidateMatches.filter { quotaEntity =>
        quotaEntity match {
          case UserClientIdEntity(_, _) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case UserDefaultClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
          case DefaultUserDefaultClientIdEntity => userMatch.isDefined && clientMatch.isDefined
          case _ => true
        }
      }
    } else {
      candidateMatches
    }

    val resultsMap: Map[QuotaEntity, Map[String, Double]] = filteredMatches.map {
      quotaEntity => quotaEntity -> quotaCache(quotaEntity).toMap
    }.toMap

    resultsMap
  }

  // Update the cache indexes for user/client quotas
  private def updateCacheIndex(quotaEntity: QuotaEntity,
                       remove: Boolean)
                      (quotaCacheIndex: QuotaCacheIndex,
                       key: CacheIndexKey): Unit = {
    if (remove) {
      val cleanup = quotaCacheIndex.get(key) match {
        case Some(quotaEntitySet) => quotaEntitySet.remove(quotaEntity); quotaEntitySet.isEmpty
        case None => false
      }
      if (cleanup) {
        quotaCacheIndex.remove(key)
      }
    } else {
      quotaCacheIndex.getOrElseUpdate(key, mutable.HashSet.empty).add(quotaEntity)
    }
  }


  def updateQuotaCache(quotaEntity: QuotaEntity, quotaRecord: QuotaRecord): Unit = inWriteLock(lock) {
    // Update the quota entity map
    val quotaValues = quotaCache.getOrElseUpdate(quotaEntity, mutable.HashMap.empty)
    val removeCache = if (quotaRecord.remove()) {
      quotaValues.remove(quotaRecord.key())
      if (quotaValues.isEmpty) {
        quotaCache.remove(quotaEntity)
        true
      } else {
        false
      }
    } else {
      quotaValues.put(quotaRecord.key(), quotaRecord.value())
      false
    }

    // Update the appropriate indexes with the entity
    val updateCacheIndexPartial: (QuotaCacheIndex, CacheIndexKey) => Unit = updateCacheIndex(quotaEntity, removeCache)
    quotaEntity match {
      case UserEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
      case DefaultUserEntity =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)

      case ClientIdEntity(clientId) =>
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))
      case DefaultClientIdEntity =>
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case UserClientIdEntity(user, clientId) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))

      case UserDefaultClientIdEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case DefaultUserClientIdEntity(clientId) =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))

      case DefaultUserDefaultClientIdEntity =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case IpEntity(ip) =>
        updateCacheIndexPartial(ipEntityIndex, SpecificIp(ip))
      case DefaultIpEntity =>
        updateCacheIndexPartial(ipEntityIndex, DefaultIp)
    }
  }
}
