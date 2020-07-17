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
 * Broker returns this error if a request could not be processed because the broker is not the leader
 * or follower for a topic partition. This could be a transient exception during leader elections and
 * reassignments. For `Produce` and other requests which are intended only for the leader, this exception
 * indicates that the broker is not the current leader. For consumer `Fetch` requests which may be
 * satisfied by a leader or follower, this exception indicates that the broker is not a replica
 * of the topic partition.
 */
@SuppressWarnings("deprecation")
public class NotLeaderOrFollowerException extends NotLeaderForPartitionException {

    private static final long serialVersionUID = 1L;

    public NotLeaderOrFollowerException() {
        super();
    }

    public NotLeaderOrFollowerException(String message) {
        super(message);
    }

    public NotLeaderOrFollowerException(Throwable cause) {
        super(cause);
    }

    public NotLeaderOrFollowerException(String message, Throwable cause) {
        super(message, cause);
    }

}
