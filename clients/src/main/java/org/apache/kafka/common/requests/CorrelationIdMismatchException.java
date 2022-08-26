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
package org.apache.kafka.common.requests;

/**
 * Raised if the correlationId in a response header does not match
 * the expected value from the request header.
 */
public class CorrelationIdMismatchException extends IllegalStateException {
    private final int requestCorrelationId;
    private final int responseCorrelationId;

    public CorrelationIdMismatchException(
        String message,
        int requestCorrelationId,
        int responseCorrelationId
    ) {
        super(message);
        this.requestCorrelationId = requestCorrelationId;
        this.responseCorrelationId = responseCorrelationId;
    }

    public int requestCorrelationId() {
        return requestCorrelationId;
    }

    public int responseCorrelationId() {
        return responseCorrelationId;
    }

}
