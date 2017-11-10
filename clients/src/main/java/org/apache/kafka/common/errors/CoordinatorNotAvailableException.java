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
 * In the context of the group coordinator, the broker returns this error code for metadata or offset commit
 * requests if the group metadata topic has not been created yet.
 *
 * In the context of the transactional coordinator, this error will be returned if the underlying transactional log
 * is under replicated or if an append to the log times out.
 */
public class CoordinatorNotAvailableException extends RetriableException {
    public static final CoordinatorNotAvailableException INSTANCE = new CoordinatorNotAvailableException();

    private static final long serialVersionUID = 1L;

    private CoordinatorNotAvailableException() {
        super();
    }

    public CoordinatorNotAvailableException(String message) {
        super(message);
    }

    public CoordinatorNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
