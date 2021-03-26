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
package org.apache.kafka.clients.admin.internals;

import java.util.OptionalInt;

/**
 * This interface is used by {@link AdminApiDriver} to bridge the gap
 * to the internal `NodeProvider` defined in
 * {@link org.apache.kafka.clients.admin.KafkaAdminClient}. However, a
 * request scope is more than just a target broker specification. It also
 * provides a way to group key lookups according to different batching
 * mechanics. See {@link AdminApiLookupStrategy#lookupScope(Object)} for
 * more detail.
 */
public interface ApiRequestScope {

    /**
     * Get the target broker ID that a request is intended for or
     * empty if the request can be sent to any broker.
     *
     * @return optional broker id
     */
    default OptionalInt destinationBrokerId() {
        return OptionalInt.empty();
    }

}
