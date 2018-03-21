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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec;
import org.apache.kafka.trogdor.fault.Kibosh;
import org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec;
import org.apache.kafka.trogdor.fault.ProcessStopFaultSpec;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.rest.WorkerStopping;
import org.apache.kafka.trogdor.task.SampleTaskSpec;
import org.apache.kafka.trogdor.workload.ProduceBenchSpec;
import org.apache.kafka.trogdor.workload.RoundTripWorkloadSpec;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;

public class JsonSerializationTest {
    @Test
    public void testDeserializationDoesNotProduceNulls() throws Exception {
        verify(new FilesUnreadableFaultSpec(0, 0, null,
            null, null, 0));
        verify(new Kibosh.KiboshControlFile(null));
        verify(new NetworkPartitionFaultSpec(0, 0, null));
        verify(new ProcessStopFaultSpec(0, 0, null, null));
        verify(new AgentStatusResponse(0, null));
        verify(new TasksResponse(null));
        verify(new WorkerDone(null, null, 0, 0, null, null));
        verify(new WorkerRunning(null, null, 0, null));
        verify(new WorkerStopping(null, null, 0, null));
        verify(new ProduceBenchSpec(0, 0, null, null,
            0, 0, null, null, null, null, null, 0, 0, "test-topic", 1, (short) 3));
        verify(new RoundTripWorkloadSpec(0, 0, null, null, null, null, null, null,
            0, null, null, 0));
        verify(new SampleTaskSpec(0, 0, null, null));
    }

    private <T> void verify(T val1) throws Exception {
        byte[] bytes = JsonUtil.JSON_SERDE.writeValueAsBytes(val1);
        Class<T> clazz = (Class<T>) val1.getClass();
        T val2 = JsonUtil.JSON_SERDE.readValue(bytes, clazz);
        for (Field field : clazz.getDeclaredFields()) {
            boolean wasAccessible = field.isAccessible();
            field.setAccessible(true);
            assertNotNull("Field " + field + " was null.", field.get(val2));
            field.setAccessible(wasAccessible);
        }
    }
};
