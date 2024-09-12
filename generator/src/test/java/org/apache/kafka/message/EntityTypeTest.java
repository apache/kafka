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

package org.apache.kafka.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(120)
public class EntityTypeTest {

    @Test
    public void testUnknownEntityType() {
        for (FieldType type : new FieldType[] {
            FieldType.StringFieldType.INSTANCE,
            FieldType.Int8FieldType.INSTANCE,
            FieldType.Int16FieldType.INSTANCE,
            FieldType.Int32FieldType.INSTANCE,
            FieldType.Int64FieldType.INSTANCE,
            new FieldType.ArrayType(FieldType.StringFieldType.INSTANCE)}) {
            EntityType.UNKNOWN.verifyTypeMatches("unknown", type);
        }
    }

    @Test
    public void testVerifyTypeMatches() {
        EntityType.TRANSACTIONAL_ID.verifyTypeMatches("transactionalIdField",
            FieldType.StringFieldType.INSTANCE);
        EntityType.TRANSACTIONAL_ID.verifyTypeMatches("transactionalIdField",
            new FieldType.ArrayType(FieldType.StringFieldType.INSTANCE));
        EntityType.PRODUCER_ID.verifyTypeMatches("producerIdField",
            FieldType.Int64FieldType.INSTANCE);
        EntityType.PRODUCER_ID.verifyTypeMatches("producerIdField",
            new FieldType.ArrayType(FieldType.Int64FieldType.INSTANCE));
        EntityType.GROUP_ID.verifyTypeMatches("groupIdField",
            FieldType.StringFieldType.INSTANCE);
        EntityType.GROUP_ID.verifyTypeMatches("groupIdField",
            new FieldType.ArrayType(FieldType.StringFieldType.INSTANCE));
        EntityType.TOPIC_NAME.verifyTypeMatches("topicNameField",
            FieldType.StringFieldType.INSTANCE);
        EntityType.TOPIC_NAME.verifyTypeMatches("topicNameField",
            new FieldType.ArrayType(FieldType.StringFieldType.INSTANCE));
        EntityType.BROKER_ID.verifyTypeMatches("brokerIdField",
            FieldType.Int32FieldType.INSTANCE);
        EntityType.BROKER_ID.verifyTypeMatches("brokerIdField",
            new FieldType.ArrayType(FieldType.Int32FieldType.INSTANCE));
    }

    private static void expectException(Runnable r) {
        assertThrows(RuntimeException.class, r::run);
    }

    @Test
    public void testVerifyTypeMismatches() {
        expectException(() -> EntityType.TRANSACTIONAL_ID.
            verifyTypeMatches("transactionalIdField", FieldType.Int32FieldType.INSTANCE));
        expectException(() -> EntityType.PRODUCER_ID.
            verifyTypeMatches("producerIdField", FieldType.StringFieldType.INSTANCE));
        expectException(() -> EntityType.GROUP_ID.
            verifyTypeMatches("groupIdField", FieldType.Int8FieldType.INSTANCE));
        expectException(() -> EntityType.TOPIC_NAME.
            verifyTypeMatches("topicNameField",
                new FieldType.ArrayType(FieldType.Int64FieldType.INSTANCE)));
        expectException(() -> EntityType.BROKER_ID.
            verifyTypeMatches("brokerIdField", FieldType.Int64FieldType.INSTANCE));
    }
}
