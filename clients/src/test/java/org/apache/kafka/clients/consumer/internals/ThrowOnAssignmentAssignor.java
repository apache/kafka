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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;

import java.util.List;

/**
 * A mock assignor which throws for {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor#onAssignment}.
 */
public class ThrowOnAssignmentAssignor extends MockPartitionAssignor {

    private final RuntimeException bookeepedException;
    private final String name;

    ThrowOnAssignmentAssignor(final List<RebalanceProtocol> supportedProtocols,
                              final RuntimeException bookeepedException,
                              final String name) {
        super(supportedProtocols);
        this.bookeepedException = bookeepedException;
        this.name = name;
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        super.onAssignment(assignment, metadata);
        throw bookeepedException;
    }

    @Override
    public String name() {
        return name;
    }
}
