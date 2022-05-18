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
package org.apache.kafka.clients;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiVersionsTest {

    @Test
    public void testMaxUsableProduceMagic() {
        ApiVersions apiVersions = new ApiVersions();
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("0", NodeApiVersions.create());
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("1", NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 2));
        assertEquals(RecordBatch.MAGIC_VALUE_V1, apiVersions.maxUsableProduceMagic());

        apiVersions.remove("1");
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());
    }

    @Test
    public void testMaxUsableProduceMagicWithRaftController() {
        ApiVersions apiVersions = new ApiVersions();
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        // something that doesn't support PRODUCE, which is the case with Raft-based controllers
        apiVersions.update("2", NodeApiVersions.create(Collections.singleton(
            new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.FETCH.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 2))));
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());
    }
}
