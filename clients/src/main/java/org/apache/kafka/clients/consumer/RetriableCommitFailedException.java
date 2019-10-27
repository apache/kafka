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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.errors.ApiException;

public class RetriableCommitFailedException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RetriableCommitFailedException(Throwable t) {
        super("Offset commit failed with a retriable exception. This is usually because the consumer is no longer " +
            "part of a group or is participating a rebalance right now. You should first call poll to complete " +
            "the rebalance and then commit the latest consumed offsets.", t);
    }

    public RetriableCommitFailedException(String message) {
        super(message);
    }

    public RetriableCommitFailedException(String message, Throwable t) {
        super(message, t);
    }
}
