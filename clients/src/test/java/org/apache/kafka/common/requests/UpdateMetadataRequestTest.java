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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.UpdateMetadataRequest.PartitionState;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UpdateMetadataRequestTest {
    @Test
    public void testBuildUpdateMetadataRequestWithSamePayload() {
        int someControllerId = 1;
        int someControllerEpoch = 1;
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        Set<UpdateMetadataRequest.Broker> liveBrokers = new HashSet<>();
        Set<UpdateMetadataRequest> generatedUpdateMetadataRequests = new HashSet<>();

        for (short version = 0; version < 5; version++) {
            AbstractRequest.Builder updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(version, someControllerId, someControllerEpoch, partitionStates, liveBrokers);
            UpdateMetadataRequest updateMetadataRequest1 = (UpdateMetadataRequest) updateMetadataRequestBuilder.build();
            Struct struct1 = updateMetadataRequest1.toStruct();

            // When we first generate the UpdateMetadataRequest for a specific version, it should not have been generated before
            Assert.assertFalse(generatedUpdateMetadataRequests.contains(updateMetadataRequest1));
            
            UpdateMetadataRequest updateMetadataRequest2 = (UpdateMetadataRequest) updateMetadataRequestBuilder.build();
            Struct struct2 = updateMetadataRequest2.toStruct();
            
            // The two UpdateMetadataRequests refer to the same object
            Assert.assertTrue(updateMetadataRequest1 == updateMetadataRequest2);
            // The two Structs refer to the same object
            Assert.assertTrue(struct1 == struct2);

            generatedUpdateMetadataRequests.add(updateMetadataRequest1);
        }
    }
}
