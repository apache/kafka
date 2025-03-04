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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 * The result of the {@link Admin#deleteStreamsGroups(Collection, DeleteStreamsGroupsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DeleteStreamsGroupsResult {

    private final DeleteConsumerGroupsResult delegate;

    DeleteStreamsGroupsResult(final Map<String, KafkaFuture<Void>> futures) {
        delegate = new DeleteConsumerGroupsResult(futures);
    }

    DeleteStreamsGroupsResult(final DeleteConsumerGroupsResult delegate) {
        this.delegate = delegate;
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     */
    public KafkaFuture<Void> all() {
        return delegate.all();
    }

    /**
     * Return a map from group id to futures which can be used to check the status of individual deletions.
     */
    public Map<String, KafkaFuture<Void>> deletedGroups() {
        return delegate.deletedGroups();
    }
}
