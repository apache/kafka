/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import java.util

import kafka.utils.{SystemTime, Logging}
import org.apache.kafka.common.cache.{Cache, TTLCache}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Shell.ShellCommandExecutor
import org.apache.kafka.common.utils.SystemTime

/**
 * Converts principal to Unix Group
 */

object PrincipalToUnixGroup {
  val GroupOutputRegex = "[ \t\n\r\f]"

  val GroupCacheExpirationTTLMillisProperty = "group.cache.ttl.millis"

  //default TTL is 1 hour.
  val GroupCacheExpirationTTLMillisDefault: Long = 1 * 60 * 60 * 1000
}

class PrincipalToUnixGroup extends PrincipalToGroup with Logging {

  private var principalToGroupsCache: Cache[KafkaPrincipal, Set[KafkaPrincipal]] = null

  /**
   *
   * @param principal
   * @return Set of Groups this principal is part of, empty Set if not part of any group.
   */
  override def toGroups(principal: KafkaPrincipal): Set[KafkaPrincipal] = {
    var groups = if (principal != null) principalToGroupsCache.get(principal) else Set.empty[KafkaPrincipal]
    if (groups == null) {
      groups = getGroups(principal)
      principalToGroupsCache.put(principal, groups)
    }
    groups
  }

  /**
   * Gets the groups by executing a shell command.
   * @param principal
   * @return
   */
  private def getGroups(principal: KafkaPrincipal): Set[KafkaPrincipal] = {
    val shellCmdExecutor = new ShellCommandExecutor(Array("bash", "-c", s"id -gn ${principal.getName} && id -Gn ${principal.getName}"), 5000)
    try {
      shellCmdExecutor.execute()
    } catch {
      case e: Exception => logger.warn(s"Failed to get groups for $principal, ignoring exception and returning empty set.", e)
    }
    val output = shellCmdExecutor.output()
    val groups = if (output != null && !output.trim.isEmpty)
      output.split(PrincipalToUnixGroup.GroupOutputRegex).map(group => new KafkaPrincipal(KafkaPrincipal.GROUP_TYPE, group)).toSet
    else
      Set.empty[KafkaPrincipal]

    groups
  }

  override def configure(configs: util.Map[String, _]) = {
    val ttlMillis: Long = if (configs !=null && configs.containsKey(PrincipalToUnixGroup.GroupCacheExpirationTTLMillisProperty))
      configs.get(PrincipalToUnixGroup.GroupCacheExpirationTTLMillisProperty).asInstanceOf[Long]
    else
      PrincipalToUnixGroup.GroupCacheExpirationTTLMillisDefault

    //The time instance is not propagated to Authorizer so right now there is no way to propagate that to this layer.
    principalToGroupsCache = new TTLCache[KafkaPrincipal, Set[KafkaPrincipal]](ttlMillis, new SystemTime())
  }
}
