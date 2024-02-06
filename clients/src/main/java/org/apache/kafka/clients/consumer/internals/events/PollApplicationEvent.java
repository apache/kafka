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
package org.apache.kafka.clients.consumer.internals.events;

public class PollApplicationEvent extends ApplicationEvent {

    private final long pollTimeMs;

    public PollApplicationEvent(final long pollTimeMs) {
        super(Type.POLL);
        this.pollTimeMs = pollTimeMs;
    }

    public long pollTimeMs() {
        return pollTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PollApplicationEvent that = (PollApplicationEvent) o;

        return pollTimeMs == that.pollTimeMs;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (pollTimeMs ^ (pollTimeMs >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "PollApplicationEvent{" +
                toStringBase() +
                ", pollTimeMs=" + pollTimeMs +
                '}';
    }
}