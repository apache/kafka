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
 * Options for {@link Admin#describeConfigs(Collection)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeConfigsOptions extends AbstractOptions<DescribeConfigsOptions> {

    private boolean includeSynonyms = false;
    private boolean includeDocumentation = false;

    /**
     * Set the timeout in milliseconds for this operation or {@code null} if the default api timeout for the
     * AdminClient should be used.
     *
     */
    // This method is retained to keep binary compatibility with 0.11
    public DescribeConfigsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * Return true if synonym configs should be returned in the response.
     */
    public boolean includeSynonyms() {
        return includeSynonyms;
    }

    /**
     * Return true if config documentation should be returned in the response.
     */
    public boolean includeDocumentation() {
        return includeDocumentation;
    }

    /**
     * Set to true if synonym configs should be returned in the response.
     */
    public DescribeConfigsOptions includeSynonyms(boolean includeSynonyms) {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    /**
     * Set to true if config documentation should be returned in the response.
     */
    public DescribeConfigsOptions includeDocumentation(boolean includeDocumentation) {
        this.includeDocumentation = includeDocumentation;
        return this;
    }
}
