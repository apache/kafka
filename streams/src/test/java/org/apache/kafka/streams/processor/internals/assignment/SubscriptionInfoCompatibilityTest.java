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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.common.message.AbstractMessageCompatibilityTest;
import org.apache.kafka.common.message.KafkaVersion;
import org.apache.kafka.message.MessageSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SubscriptionInfoCompatibilityTest extends AbstractMessageCompatibilityTest {

    @Parameterized.Parameters(name= "{0}: old={1}, new={2}")
    public static Collection<Object[]> parameters() throws IOException {
        return messagePairs("streams/src/main/resources/common/message/", ".json");
    }

    private final String messageName;
    private final KafkaVersion oldVersion;
    private final MessageSpec oldSpec;
    private final KafkaVersion newVersion;
    private final MessageSpec newSpec;

    public SubscriptionInfoCompatibilityTest(String messageName, KafkaVersion oldVersion, MessageSpec oldSpec, KafkaVersion newVersion, MessageSpec newSpec) {
        this.messageName = messageName;
        this.oldVersion = oldVersion;
        this.oldSpec = oldSpec;
        this.newVersion = newVersion;
        this.newSpec = newSpec;
    }

    @Test
    public void testCompatible() {
        validateCompatibility(messageName, oldVersion, oldSpec, newVersion, newSpec);
    }
}
