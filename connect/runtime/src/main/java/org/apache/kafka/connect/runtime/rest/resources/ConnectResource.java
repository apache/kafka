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
package org.apache.kafka.connect.runtime.rest.resources;

import java.util.concurrent.TimeUnit;

/**
 * This interface defines shared logic for all Connect REST resources.
 */
public interface ConnectResource {

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    long DEFAULT_REST_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(90);

    /**
     * Set how long the resource will await the completion of each request before returning a 500 error.
     * If the resource does not perform any operations that can be expected to block under reasonable
     * circumstances, this can be implemented as a no-op.
     * @param requestTimeoutMs the new timeout in milliseconds; must be positive
     */
    void requestTimeout(long requestTimeoutMs);

}
