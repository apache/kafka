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
import org.apache.kafka.common.quota.{ClientQuotaEntity, ClientQuotaFilterComponent}

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable
import scala.jdk.CollectionConverters._


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


/**
 * Maintains a cache of QuotaEntity and their respective quotas.
 *
 * The main cache is structured like:
 *
 * <pre>
 * {
 *   (user:alice) -> {consumer_byte_rate: 10000},
 *   (user:alice,client:x) -> {consumer_byte_rate: 8000, producer_byte_rate: 8000}
 * }
 * </pre>
 *
 * In addition to this cache, this class maintains three indexes for the three supported entity types (user, client,
 * and IP). These indexes map a part of an entity to the list of all QuotaEntity which contain that entity. For example:
 *
 * <pre>
 * {
 *   SpecificUser(alice) -> [(user:alice), (user:alice,client:x)],
 *   DefaultUser -> [(user:default), (user:default, client:x)]
 * }
 * </pre>
 *
 * These indexes exist to support the flexible lookups needed by DescribeClientQuota RPC
 */
class ClientQuotaCache {
  private type QuotaCacheIndex = mutable.HashMap[CacheIndexKey, mutable.HashSet[QuotaEntity]]

  private val quotaCache = new mutable.HashMap[QuotaEntity, mutable.Map[String, Double]]

  // We need three separate indexes because we also support wildcard lookups on entity type.
  private val userEntityIndex = new QuotaCacheIndex
  private val clientIdEntityIndex = new QuotaCacheIndex
  private val ipEntityIndex = new QuotaCacheIndex

  private val lock = new ReentrantReadWriteLock()

  /**
   * Return quota entries for a given filter. These entries are returned from an in-memory cache and may not reflect
   * the latest state of the quotas according to the controller. If a filter is given for an unsupported entity type
   * or an invalid combination of entity types, this method will throw an exception.
   *
   * @param filters       A collection of quota filters (entity type and a match clause).
   * @param strict        True if we should only return entities which match all the filter clauses and have no
   *                      additional unmatched parts.
   * @return              A mapping of quota entities along with their quota values.
   */
  def describeClientQuotas(filters: Seq[ClientQuotaFilterComponent], strict: Boolean):
      Map[ClientQuotaEntity, Map[String, Double]] = inReadLock(lock) {
    describeClientQuotasInternal(filters, strict).map { case (entity, value) => convertEntity(entity) -> value}
  }

  // Visible for testing (QuotaEntity is nicer for assertions in test code)
  private[metadata] def describeClientQuotasInternal(filters: Seq[ClientQuotaFilterComponent], strict: Boolean):
      Map[QuotaEntity, Map[String, Double]] = inReadLock(lock) {

    // Do some preliminary validation of the filter types and convert them to correct QuotaMatch type
    val entityFilters = mutable.HashMap.empty[String, QuotaMatch]
    filters.foreach { component =>
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
    }

    // Special case for non-strict empty filter, match everything
    if (filters.isEmpty && !strict) {
      val allResults: Map[QuotaEntity, Map[String, Double]] = quotaCache.map {
        entry => entry._1 -> entry._2.toMap
      }.toMap
      return allResults
    }

    if (entityFilters.isEmpty) {
      return Map.empty
    }

    // We do not allow IP filters to be combined with user or client filters
    val matchingEntities: Set[QuotaEntity] = if (entityFilters.contains(ClientQuotaEntity.IP)) {
      if (entityFilters.size > 1) {
        throw new InvalidRequestException("Invalid entity filter component combination, IP filter component should " +
          "not be used with User or ClientId filter component.")
      }
      val ipMatch = entityFilters.get(ClientQuotaEntity.IP)
      ipMatch.fold(Set.empty[QuotaEntity]) {
          case ExactMatch(ip) => ipEntityIndex.getOrElse(SpecificIp(ip), Set.empty).toSet
          case DefaultMatch => ipEntityIndex.getOrElse(DefaultIp, Set.empty).toSet
          case TypeMatch => ipEntityIndex.values.flatten.toSet
      }
    } else if (entityFilters.contains(ClientQuotaEntity.USER) || entityFilters.contains(ClientQuotaEntity.CLIENT_ID)) {
      // If either are present, check both user and client indexes
      val userMatch = entityFilters.get(ClientQuotaEntity.USER)
      val userIndexMatches = userMatch.fold(Set.empty[QuotaEntity]) {
        case ExactMatch(user) => userEntityIndex.getOrElse(SpecificUser(user), Set.empty).toSet
        case DefaultMatch => userEntityIndex.getOrElse(DefaultUser, Set.empty).toSet
        case TypeMatch => userEntityIndex.values.flatten.toSet
      }

      val clientMatch = entityFilters.get(ClientQuotaEntity.CLIENT_ID)
      val clientIndexMatches = clientMatch.fold(Set.empty[QuotaEntity]) {
        case ExactMatch(clientId) => clientIdEntityIndex.getOrElse(SpecificClientId(clientId), Set.empty).toSet
        case DefaultMatch => clientIdEntityIndex.getOrElse(DefaultClientId, Set.empty).toSet
        case TypeMatch => clientIdEntityIndex.values.flatten.toSet
      }

      val candidateMatches = if (userMatch.isDefined && clientMatch.isDefined) {
        userIndexMatches.intersect(clientIndexMatches)
      } else if (userMatch.isDefined) {
        userIndexMatches
      } else {
        clientIndexMatches
      }

      if (strict) {
        // If in strict mode, we need to remove any matches with unspecified entity types. This only applies to results
        // with more than one entity part (i.e., user and clientId)
        candidateMatches.filter { quotaEntity =>
          quotaEntity match {
            case ExplicitUserExplicitClientIdEntity(_, _) => userMatch.isDefined && clientMatch.isDefined
            case DefaultUserExplicitClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
            case ExplicitUserDefaultClientIdEntity(_) => userMatch.isDefined && clientMatch.isDefined
            case DefaultUserDefaultClientIdEntity => userMatch.isDefined && clientMatch.isDefined
            case _ => true
          }
        }
      } else {
        candidateMatches
      }
    } else {
      // ClientQuotaEntity.isValidEntityType check above should prevent any unknown entity types
      throw new IllegalStateException(s"Unexpected handling of ${entityFilters} after filter validation")
    }

    val resultsMap: Map[QuotaEntity, Map[String, Double]] = matchingEntities.map {
      quotaEntity => {
        quotaCache.get(quotaEntity) match {
          case Some(quotas) => quotaEntity -> quotas.toMap
          case None => quotaEntity -> Map.empty[String, Double]
        }
      }
    }.toMap

    resultsMap
  }

  private def convertEntity(entity: QuotaEntity): ClientQuotaEntity = {
    val entityMap = entity match {
      case IpEntity(ip) => Map(ClientQuotaEntity.IP -> ip)
      case DefaultIpEntity => Map(ClientQuotaEntity.IP -> null)
      case UserEntity(user) => Map(ClientQuotaEntity.USER -> user)
      case DefaultUserEntity => Map(ClientQuotaEntity.USER -> null)
      case ClientIdEntity(clientId) => Map(ClientQuotaEntity.CLIENT_ID -> clientId)
      case DefaultClientIdEntity => Map(ClientQuotaEntity.CLIENT_ID -> null)
      case ExplicitUserExplicitClientIdEntity(user, clientId) =>
        Map(ClientQuotaEntity.USER -> user, ClientQuotaEntity.CLIENT_ID -> clientId)
      case ExplicitUserDefaultClientIdEntity(user) =>
        Map(ClientQuotaEntity.USER -> user, ClientQuotaEntity.CLIENT_ID -> null)
      case DefaultUserExplicitClientIdEntity(clientId) =>
        Map(ClientQuotaEntity.USER -> null, ClientQuotaEntity.CLIENT_ID -> clientId)
      case DefaultUserDefaultClientIdEntity =>
        Map(ClientQuotaEntity.USER -> null, ClientQuotaEntity.CLIENT_ID -> null)
    }
    new ClientQuotaEntity(entityMap.asJava)
  }

  // Update the cache indexes
  private def updateCacheIndex(quotaEntity: QuotaEntity,
                               remove: Boolean)
                              (quotaCacheIndex: QuotaCacheIndex,
                               key: CacheIndexKey): Unit = {
    if (remove) {
      val needsCleanup = quotaCacheIndex.get(key) match {
        case Some(quotaEntitySet) =>
          quotaEntitySet.remove(quotaEntity)
          quotaEntitySet.isEmpty
        case None => false
      }
      if (needsCleanup) {
        quotaCacheIndex.remove(key)
      }
    } else {
      quotaCacheIndex.getOrElseUpdate(key, mutable.HashSet.empty).add(quotaEntity)
    }
  }

  /**
   * Update the quota cache with the given entity and quota key/value. If remove is set, the value is ignore and
   * the quota entry is removed for the given key. No validation on quota keys is performed here, it is assumed
   * that the caller has already done this.
   *
   * @param entity    A quota entity, either a specific entity or the default entity for the given type(s)
   * @param key       The quota key
   * @param value     The quota value
   * @param remove    True if we should remove the given quota key from the entity's quota cache
   */
  def updateQuotaCache(entity: QuotaEntity, key: String, value: Double, remove: Boolean): Unit = inWriteLock(lock) {
    val quotaValues = quotaCache.getOrElseUpdate(entity, mutable.HashMap.empty)
    val removeFromIndex = if (remove) {
      quotaValues.remove(key)
      if (quotaValues.isEmpty) {
        quotaCache.remove(entity)
        true
      } else {
        false
      }
    } else {
      quotaValues.put(key, value)
      false
    }

    // Update the appropriate indexes with the entity
    val updateCacheIndexPartial: (QuotaCacheIndex, CacheIndexKey) => Unit = updateCacheIndex(entity, removeFromIndex)
    entity match {
      case UserEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
      case DefaultUserEntity =>
        updateCacheIndexPartial(userEntityIndex, DefaultUser)

      case ClientIdEntity(clientId) =>
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))
      case DefaultClientIdEntity =>
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case ExplicitUserExplicitClientIdEntity(user, clientId) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, SpecificClientId(clientId))

      case ExplicitUserDefaultClientIdEntity(user) =>
        updateCacheIndexPartial(userEntityIndex, SpecificUser(user))
        updateCacheIndexPartial(clientIdEntityIndex, DefaultClientId)

      case DefaultUserExplicitClientIdEntity(clientId) =>
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

  override def toString = s"ClientQuotaCache($quotaCache)"
}
