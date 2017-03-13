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

package kafka.security.auth

import kafka.network.RequestChannel.Session
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
 * Top level interface that all pluggable authorizers must implement. Kafka will read the `authorizer.class.name` config
 * value at startup time, create an instance of the specified class using the default constructor, and call its
 * `configure` method.
 *
 * From that point onwards, every client request will first be routed to the `authorize` method and the request will only
 * be authorized if the method returns true.
 *
 * If `authorizer.class.name` has no value specified, then no authorization will be performed, and all operations are
 * permitted.
 */
trait Authorizer extends Configurable {

  /**
   * @param session The session being authenticated.
   * @param operation Type of operation client is trying to perform on resource.
   * @param resource Resource the client is trying to access.
   * @return true if the operation should be permitted, false otherwise
   */
  def authorize(session: Session, operation: Operation, resource: Resource): Boolean

  /**
   * add the acls to resource, this is an additive operation so existing acls will not be overwritten, instead these new
   * acls will be added to existing acls.
   * @param acls set of acls to add to existing acls
   * @param resource the resource to which these acls should be attached.
   */
  def addAcls(acls: Set[Acl], resource: Resource): Unit

  /**
   * remove these acls from the resource.
   * @param acls set of acls to be removed.
   * @param resource resource from which the acls should be removed.
   * @return true if some acl got removed, false if no acl was removed.
   */
  def removeAcls(acls: Set[Acl], resource: Resource): Boolean

  /**
   * remove a resource along with all of its acls from acl store.
   * @param resource
   * @return
   */
  def removeAcls(resource: Resource): Boolean

  /**
   * get set of acls for this resource
   * @param resource
   * @return empty set if no acls are found, otherwise the acls for the resource.
   */
  def getAcls(resource: Resource): Set[Acl]

  /**
   * get the acls for this principal.
   * @param principal
   * @return empty Map if no acls exist for this principal, otherwise a map of resource -> acls for the principal.
   */
  def getAcls(principal: KafkaPrincipal): Map[Resource, Set[Acl]]

  /**
   * gets the map of resource to acls for all resources.
   */
  def getAcls(): Map[Resource, Set[Acl]]

  /**
   * Closes this instance.
   */
  def close(): Unit

}

