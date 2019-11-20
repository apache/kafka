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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;

import static org.apache.kafka.common.protocol.ApiKeys.ADD_OFFSETS_TO_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.ADD_PARTITIONS_TO_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.ALTER_CONFIGS;
import static org.apache.kafka.common.protocol.ApiKeys.ALTER_REPLICA_LOG_DIRS;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_PARTITIONS;
import static org.apache.kafka.common.protocol.ApiKeys.DELETE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.DELETE_RECORDS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_ACLS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_LOG_DIRS;
import static org.apache.kafka.common.protocol.ApiKeys.END_TXN;
import static org.apache.kafka.common.protocol.ApiKeys.FETCH;
import static org.apache.kafka.common.protocol.ApiKeys.LIST_OFFSETS;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_FOR_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.apache.kafka.common.protocol.ApiKeys.TXN_OFFSET_COMMIT;
import static org.apache.kafka.common.protocol.ApiKeys.WRITE_TXN_MARKERS;
import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.COMPACT_BYTES;
import static org.apache.kafka.common.protocol.types.Type.COMPACT_NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoUtilsTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(30000);

    @Test
    public void testDelayedAllocationSchemaDetection() throws Exception {
        // Verifies that schemas known to retain a reference to the underlying byte buffer
        // are correctly detected.  This is important because we cannot reuse buffers containing
        // requests of these types until the request has been completely processed.
        for (ApiKeys key : ApiKeys.values()) {
            switch (key) {
                case PRODUCE:
                case SASL_AUTHENTICATE:
                    assertTrue(key + " should require delayed allocation",
                        ApiMessageType.fromApiKey(key.id).requestContainsZeroCopyFields());
                    break;
                default:
                    assertFalse(key + " should not require delayed allocation",
                        ApiMessageType.fromApiKey(key.id).requestContainsZeroCopyFields());
                    break;
            }
        }
    }

    /**
     * There are still some requests that use manual serialization via Struct objects rather than
     * automatically generated code.  Bytes fields in these requests will always be treated as
     * zero-copy, because the Struct.java code always uses ByteBuffers here.  This test checks
     * that bytes fields in these requests are marked as zero-copy to reflect this fact.
     */
    @Test
    public void testRequestsUsingManualSerializationDoNotContainBytesFields() {
        for (ApiKeys key : Arrays.asList(
                ADD_OFFSETS_TO_TXN,
                ADD_PARTITIONS_TO_TXN,
                ALTER_CONFIGS,
                ALTER_REPLICA_LOG_DIRS,
                CREATE_ACLS,
                CREATE_PARTITIONS,
                DELETE_ACLS,
                DELETE_RECORDS,
                DESCRIBE_ACLS,
                DESCRIBE_CONFIGS,
                DESCRIBE_LOG_DIRS,
                END_TXN,
                FETCH,
                LIST_OFFSETS,
                OFFSET_FOR_LEADER_EPOCH,
                PRODUCE,
                TXN_OFFSET_COMMIT,
                WRITE_TXN_MARKERS)) {
            if (!ApiMessageType.fromApiKey(key.id).requestContainsZeroCopyFields()) {
                for (short v = key.oldestVersion(); v <= key.latestVersion(); v++) {
                    key.requestSchemas[v].walk(new Schema.Visitor() {
                        @Override
                        public void visit(Type field) {
                            if (field == BYTES || field == NULLABLE_BYTES || field == RECORDS ||
                                    field == COMPACT_BYTES || field == COMPACT_NULLABLE_BYTES) {
                                throw new RuntimeException("Request using manual serialization " + key +
                                    " must not include byte buffers unless they are marked as zeroCopy.");
                            }
                        }
                    });
                }
            }
        }
    }
}
