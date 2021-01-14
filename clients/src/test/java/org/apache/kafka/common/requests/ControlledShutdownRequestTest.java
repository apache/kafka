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

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ControlledShutdownRequestTest {

    @Test
    public void testUnsupportedVersion() {
        ControlledShutdownRequest.Builder builder = new ControlledShutdownRequest.Builder(
                new ControlledShutdownRequestData().setBrokerId(1),
                (short) (CONTROLLED_SHUTDOWN.latestVersion() + 1));
        assertThrows(UnsupportedVersionException.class, builder::build);
    }

    @Test
    public void testGetErrorResponse() {
        for (short version = CONTROLLED_SHUTDOWN.oldestVersion(); version < CONTROLLED_SHUTDOWN.latestVersion(); version++) {
            ControlledShutdownRequest.Builder builder = new ControlledShutdownRequest.Builder(
                    new ControlledShutdownRequestData().setBrokerId(1), version);
            ControlledShutdownRequest request = builder.build();
            ControlledShutdownResponse response = request.getErrorResponse(0,
                    new ClusterAuthorizationException("Not authorized"));
            assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, response.error());
        }
    }

}
