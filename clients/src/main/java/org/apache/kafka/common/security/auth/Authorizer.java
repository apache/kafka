/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.Configurable;

import java.util.Map;
import java.util.Set;

/**
 * Top level interface that all plugable authorizer must implement. Kafka server will read "authorizer.class" config
 * value at startup time, create an instance of the specified class and call initialize method.
 * authorizer.class must be a class that implements this interface.
 * If authorizer.class has no value specified no authorization will be performed.
 *
 * From that point onwards, every client request will first be routed to authorize method and the request will only be
 * authorized if the method returns true.
 */
public interface Authorizer extends Configurable {

    /**
     * @param session   The session being authenticated.
     * @param operation Type of operation client is trying to perform on resource.
     * @param resource  Resource the client is trying to access.
     * @return
     *
     * @throws org.apache.kafka.common.errors.InvalidResourceException if resource does not exist
     * @throws org.apache.kafka.common.errors.InvalidOperationException if requested operation is not
     *          supported on the resource
     */
    public boolean authorize(Session session, Operation operation, Resource resource);

    /**
     * implementation specific description, like, supported principal types.
     *
     * @return implementation specific description.
     */
    public String description();

    /**
     * add the acls to resource, this is an additive operation so existing acls will not be overwritten, instead these new
     * acls will be added to existing acls.
     *
     * @param acls     set of acls to add to existing acls
     * @param resource the resource to which these acls should be attached.
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to add acls for the resource
     * @throws org.apache.kafka.common.errors.InvalidResourceException if resource does not exist
     * @throws org.apache.kafka.common.errors.InvalidAclException if an invalid acl is being added
     */
    public void addAcls(Set<Acl> acls, Resource resource);

    /**
     * remove these acls from the resource.
     *
     * @param acls     set of acls to be removed.
     * @param resource resource from which the acls should be removed.
     * @return true if some acl got removed, false if no acl was removed.
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to remove acls for the resource
     * @throws org.apache.kafka.common.errors.InvalidResourceException if resource does not exist
     * @throws org.apache.kafka.common.errors.InvalidAclException if an invalid acl is being removed
     */
    public boolean removeAcls(Set<Acl> acls, Resource resource);

    /**
     * remove a resource along with all of its acls from acl store.
     *
     * @param resource
     * @return
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to remove acls for the resource
     * @throws org.apache.kafka.common.errors.InvalidResourceException if resource does not exist
     */
    public boolean removeAcls(Resource resource);

    /**
     * get set of acls for this resource
     *
     * @param resource
     * @return empty set if no acls are found, otherwise the acls for the resource.
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to access acls for the resource
     * @throws org.apache.kafka.common.errors.InvalidResourceException if resource does not exist
     */
    public Set<Acl> acls(Resource resource);

    /**
     * get the acls for this principal.
     *
     * @param principal
     * @return empty Map if no acls exist for this principal, otherwise a map of resource -> acls for the principal.
     *
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to access acls for the principal
     * @throws org.apache.kafka.common.errors.InvalidPrincipalException if principal is invalid
     */
    public Map<Resource, Set<Acl>> acls(KafkaPrincipal principal);

    /**
     * gets the map of resource to acls for all resources.
     */
    public Map<Resource, Set<Acl>> acls();

    /**
     * Closes this instance.
     */
    public void close();

}
