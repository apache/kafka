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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.errors.RetriableException;

/**
 * Exception thrown when an offset commit fails with a retriable error.
 * This exception is generated on the client side upon receiving retriable error codes
 * from the Group Coordinator in a commit response.
 * <p>
 * Unlike {@link CommitFailedException}, this exception indicates that the commit
 * can be retried. The consumer should attempt to commit the offsets again.
 * </p>
 */
@InterfaceAudience.Public
public class RetriableCommitFailedException extends RetriableException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new RetriableCommitFailedException with the specified cause.
     *
     * @param t The cause of the exception
     */
    public RetriableCommitFailedException(Throwable t) {
        super("Offset commit failed with a retriable exception. You should retry committing " +
                "the latest consumed offsets.", t);
    }

    /**
     * Constructs a new RetriableCommitFailedException with the specified detail message.
     *
     * @param message The detail message
     */
    public RetriableCommitFailedException(String message) {
        super(message);
    }

    /**
     * Constructs a new RetriableCommitFailedException with the specified detail message and cause.
     *
     * @param message The detail message
     * @param t The cause of the exception
     */
    public RetriableCommitFailedException(String message, Throwable t) {
        super(message, t);
    }
}
