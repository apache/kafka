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
package org.apache.kafka.connect.mirror;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MirrorSourceTaskResetBehaviorTest {

    @Test
    void defaultsToFailFastWhenNotSet() {
        assertEquals("fail-fast", MirrorSourceTask.resolveResetBehavior(Collections.emptyMap()));
    }

    @Test
    void respectsExplicitFailFast() {
        Map<String, String> props = Collections.singletonMap("topic.reset.behavior", "fail-fast");
        assertEquals("fail-fast", MirrorSourceTask.resolveResetBehavior(props));
    }

    @Test
    void respectsExplicitSelfHeal() {
        Map<String, String> props = Collections.singletonMap("topic.reset.behavior", "self-heal");
        assertEquals("self-heal", MirrorSourceTask.resolveResetBehavior(props));
    }

    @Test
    void fallsBackToFailFastOnUnrecognizedValue() {
        Map<String, String> props = Collections.singletonMap("topic.reset.behavior", "bogus");
        assertEquals("fail-fast", MirrorSourceTask.resolveResetBehavior(props));
    }
}