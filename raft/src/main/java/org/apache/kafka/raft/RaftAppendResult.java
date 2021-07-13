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
package org.apache.kafka.raft;

import java.util.Optional;

/**
 * Result of append operation to raft log
 */
public class RaftAppendResult {
    private final Optional<Long> offset;
    private final Type type;
    private RaftAppendResult(Type type, Optional<Long> offset) {
        this.type = type;
        this.offset = offset;
    }

    public enum Type {
        SUCCESS, REJECTED, FAILED
    }

    public long offset() {
        if (appendSuccess())
            return offset.get();
        else if (appendRejected())
            throw new IllegalStateException("Can not get offset because the epoch doesn't match or we are not the current leader");
        else
            throw new IllegalStateException("Can not get offset because we failed to allocate memory to write the the batch");
    }

    /**
     * @return True if we successfully scheduled the append
     */
    public boolean appendSuccess() {
        return type == Type.SUCCESS;
    }

    /**
     * @return True if the append is rejected because the epoch doesn't match or we are not the current leader
     */
    public boolean appendRejected() {
        return type == Type.REJECTED;
    }

    /**
     * @return True if the append is failed because we failed to allocate memory to write the the batch (backpressure case)
     */
    public boolean appendFailed() {
        return type == Type.REJECTED;
    }

    public static RaftAppendResult success(long offset) {
        return new RaftAppendResult(Type.SUCCESS, Optional.of(offset));
    }

    public static RaftAppendResult rejected() {
        return new RaftAppendResult(Type.REJECTED, Optional.empty());
    }

    public static RaftAppendResult failed() {
        return new RaftAppendResult(Type.FAILED, Optional.empty());
    }
}
