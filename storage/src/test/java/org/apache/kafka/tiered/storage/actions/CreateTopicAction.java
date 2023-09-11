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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.specs.TopicSpec;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createTopicConfigForRemoteStorage;

public final class CreateTopicAction implements TieredStorageTestAction {

    private final TopicSpec spec;

    public CreateTopicAction(TopicSpec spec) {
        this.spec = spec;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws ExecutionException, InterruptedException {
        boolean enableRemoteStorage = true;
        Map<String, String> topicConfigs = createTopicConfigForRemoteStorage(
                enableRemoteStorage, spec.getMaxBatchCountPerSegment());
        topicConfigs.putAll(spec.getProperties());

        spec.getProperties().clear();
        spec.getProperties().putAll(topicConfigs);
        context.createTopic(spec);
    }

    @Override
    public void describe(PrintStream output) {
        output.println("create topic: " + spec);
    }
}
