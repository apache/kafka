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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * Options for {@link Admin#describeTopics(Collection)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeTopicsOptions extends AbstractOptions<DescribeTopicsOptions> {

    private boolean includeAuthorizedOperations;

    /**
     * Set the timeout in milliseconds for this operation or {@code null} if the default api timeout for the
     * AdminClient should be used.
     *
     */
    // This method is retained to keep binary compatibility with 0.11
    public DescribeTopicsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public DescribeTopicsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }

}
