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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;

import java.util.Objects;

/**
 * This event is sent by the {@link ConsumerNetworkThread consumer's network thread} to the application thread
 * so that when the user calls the {@link Consumer#groupMetadata()} API, the information is up-to-date. The
 * information for the current state of the group member is managed on the consumer network thread and thus
 * requires this interplay between threads.
 */
public class GroupMetadataUpdatedEvent extends BackgroundEvent {

    private final ConsumerGroupMetadata groupMetadata;

    public GroupMetadataUpdatedEvent(ConsumerGroupMetadata groupMetadata) {
        super(Type.GROUP_METADATA_UPDATED);
        this.groupMetadata = Objects.requireNonNull(groupMetadata, "ConsumerGroupMetadata must be non-null");
    }

    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        GroupMetadataUpdatedEvent that = (GroupMetadataUpdatedEvent) o;

        return groupMetadata.equals(that.groupMetadata);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + groupMetadata.hashCode();
        return result;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", groupMetadata=" + groupMetadata;
    }

    @Override
    public String toString() {
        return "GroupMetadataUpdatedEvent{" +
                toStringBase() +
                '}';
    }

}
