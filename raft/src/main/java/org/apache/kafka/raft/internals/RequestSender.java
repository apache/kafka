/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;

import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Interface for sending KRaft requests.
 *
 * Responsible for managing the connection state and sending request when the connection is
 * available.
 */
public interface RequestSender {
    /**
     * The name of the listener used for sending request.
     *
     * This is generally the default (first) listener.
     */
    ListenerName listenerName();

    /**
     * Send a KRaft request to the destination.
     *
     * @param destination the destination for the request
     * @param requestSupplier the default constructor for the request
     * @param currentTimeMs the current time
     * @return the request timeout if the request was sent; otherwise {@code Optional.empty()}
     */
    OptionalLong send(
        Node destination,
        Supplier<ApiMessage> requestSupplier,
        long currentTimeMs
    );
}
