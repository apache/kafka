/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.protocol.types.Type;

/**
 * Interface for protocols supported by an implementation of {@link GroupCoordinator}. This class provides
 * the metadata and assignment schemas used by instances of the group protocol. From a
 * high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * This class defines the format of the metadata used in group registration and the format of the
 * state assigned by the leader. It is up to implementations to define their own way to version
 * these schemas. Typically, this will involve embedding version information in the metadata/assignment
 * schemas. Note that since metadata/assignment data is opaque to the coordinator, the leader will
 * be chosen randomly from among the members of the group. This means that to support rolling upgrade
 * scenarios, protocols must be both backwards and forwards compatible.
 *
 * @param <M> Type of Metadata which each member provides on group registration
 * @param <A> Type of Assignment which leader provides to each member
 */
public interface GroupProtocol<M, A> {

    /**
     * User-friendly name for the group protocol (e.g. "consumer" or "copycat").
     * @return Non-null name
     */
    String name();

    /**
     * Schema for member metadata.
     * @return the schema
     */
    GenericType<M> metadataSchema();

    /**
     * Schema for member assignment.
     * @return the schema
     */
    GenericType<A> assignmentSchema();


    // TODO: Make Type generic and remove this extension
    abstract class GenericType<T> extends Type {
        public abstract T validate(Object obj);
    }

}
