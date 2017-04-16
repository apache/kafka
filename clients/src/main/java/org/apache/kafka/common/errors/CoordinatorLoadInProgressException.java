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
package org.apache.kafka.common.errors;

/**
 * In the context of the group coordinator, the broker returns this error code for any coordinator request if
 * it is still loading the group metadata (e.g. after a leader change for that group metadata topic partition).
 *
 * In the context of the transactional coordinator, this error will be returned if there is a pending transactional
 * request with the same transactional id, or if the transaction cache is currently being populated from the transaction
 * log.
 */
public class CoordinatorLoadInProgressException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public CoordinatorLoadInProgressException(String message) {
        super(message);
    }

    public CoordinatorLoadInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

}
