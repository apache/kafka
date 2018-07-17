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

package org.apache.kafka.castle.role;

import org.apache.kafka.castle.action.Action;
import org.apache.kafka.castle.cloud.Cloud;
import org.apache.kafka.castle.cloud.MockCloud;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MockCloudRole implements Role {
    @Override
    public int initialDelayMs() {
        return 0;
    }

    @Override
    public Collection<Action> createActions(String nodeName) {
        return Collections.emptyList();
    }

    @Override
    public Cloud cloud(ConcurrentHashMap<String, Cloud> cloudCache) {
        MockCloud.Settings settings = new MockCloud.Settings();
        return cloudCache.computeIfAbsent(settings.toString(), new Function<String, Cloud>() {
            @Override
            public Cloud apply(String s) {
                return new MockCloud(settings);
            }
        });
    }
}
