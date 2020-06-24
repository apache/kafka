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

package org.apache.kafka.server.auditor;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * An auditor class that can be used to hook into the request after its completion and do auditing tasks, such as
 * logging to a special file or sending information about the request to external systems.
 * Threading model:
 * <ul>
 *     <li>The auditor implementation must be thread-safe.</li>
 *     <li>The auditor implementation is expected to be asynchronous with low latency as it is used in performance
 *     sensitive areas, such as in handling produce requests.</li>
 *     <li>Any threads or thread pools used for processing remote operations asynchronously can be started during
 *     start(). These threads must be shutdown during close().</li>
 * </ul>
 */
@InterfaceStability.Evolving
public interface Auditor extends Configurable, AutoCloseable {

    /**
     * Called on request completion before returning the response to the client. It allows auditing multiple resources
     * in the request, such as multiple topics being created.
     * @param event is the request specific data passed down to the auditor. It may be null if there are no specific
     *              information is available for the given audited event type.
     * @param requestContext contains metadata to the request.
     */
    void audit(AuditEvent event, AuthorizableRequestContext requestContext);
}
