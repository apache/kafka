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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.Optional;

@InterfaceStability.Evolving
public class AclCreateResult {
    public static final AclCreateResult SUCCESS = new AclCreateResult();

    private final ApiException exception;

    private AclCreateResult() {
        this(null);
    }

    public AclCreateResult(ApiException exception) {
        this.exception = exception;
    }

    /**
     * Returns any exception during create. If exception is empty, the request has succeeded.
     */
    public Optional<ApiException> exception() {
        return exception == null ? Optional.empty() : Optional.of(exception);
    }
}
