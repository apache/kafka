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

/**
 * Options for {@link Admin#describeCluster()}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeClusterOptions extends AbstractOptions<DescribeClusterOptions> {

    private boolean includeAuthorizedOperations;

    private boolean includeFencedBrokers;

    /**
     * Set the timeout in milliseconds for this operation or {@code null} if the default api timeout for the
     * AdminClient should be used.
     *
     */
    // This method is retained to keep binary compatibility with 0.11
    public DescribeClusterOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public DescribeClusterOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    public DescribeClusterOptions includeFencedBrokers(boolean includeFencedBrokers) {
        this.includeFencedBrokers = includeFencedBrokers;
        return this;
    }

    /**
     * Specify if authorized operations should be included in the response.  Note that some
     * older brokers cannot not supply this information even if it is requested.
     */
    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }

    /**
     * Specify if fenced brokers should be included in the response.  Note that some
     * older brokers cannot not supply this information even if it is requested.
     */
    public boolean includeFencedBrokers() {
        return includeFencedBrokers;
    }
}
