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
 * Interface for protocols supported by an implementation of {@link GroupCoordinator}. In Kafka's group
 * management, a protocol consists of a name, version, and some opaque metadata. The coordinator accepts
 * the protocols from each group member and chooses one which all members can support (based on the name
 * and version). The associated metadata for each protocol is then forwarded to all members of the group.
 *
 */
public interface GroupProtocol<M, S> {

    String name();

    GenType<M> metadataSchema();

    GenType<S> stateSchema();

    abstract class GenType<T> extends Type {
        public abstract T validate(Object obj);
    }

}
