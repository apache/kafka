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

import java.util.HashSet;
import java.util.Set;


/**
 * The client has attempted to perform an operation on an invalid topic.
 * For example the topic name is too long, contains invalid characters etc.
 * This exception is not retriable because the operation won't suddenly become valid.
 *
 * @see UnknownTopicOrPartitionException
 */
public class InvalidTopicException extends ApiException {
    private static final long serialVersionUID = 1L;

    private final Set<String> invalidTopics;

    public InvalidTopicException() {
        super();
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message, Throwable cause) {
        super(message, cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message) {
        super(message);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Throwable cause) {
        super(cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Set<String> invalidTopics) {
        super("Invalid topics: " + invalidTopics);
        this.invalidTopics = invalidTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }
}
