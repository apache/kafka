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

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.LiControlledShutdownSkipSafetyCheckResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collections;
import java.util.Map;

public class LiControlledShutdownSkipSafetyCheckResponse extends AbstractResponse {
    /**
     * Possible error codes:
     * <p>
     * UNKNOWN(-1) (this is because IllegalStateException may be thrown in `KafkaController.shutdownBroker`, it would be good to improve this)
     * STALE_CONTROLLER_EPOCH(11)
     */
    private final LiControlledShutdownSkipSafetyCheckResponseData data;

    public LiControlledShutdownSkipSafetyCheckResponse(LiControlledShutdownSkipSafetyCheckResponseData data) {
        super(ApiKeys.LI_CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK);
        this.data = data;
    }

    public static LiControlledShutdownSkipSafetyCheckResponse parse(ByteBuffer buffer, short version) {
        return new LiControlledShutdownSkipSafetyCheckResponse(new LiControlledShutdownSkipSafetyCheckResponseData(
            new ByteBufferAccessor(buffer), version));
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(error(), 1);
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public LiControlledShutdownSkipSafetyCheckResponseData data() {
        return data;
    }

    public static LiControlledShutdownSkipSafetyCheckResponse prepareResponse(Errors error) {
        LiControlledShutdownSkipSafetyCheckResponseData data = new LiControlledShutdownSkipSafetyCheckResponseData();
        data.setErrorCode(error.code());
        return new LiControlledShutdownSkipSafetyCheckResponse(data);
    }
}
