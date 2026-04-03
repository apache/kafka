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
import org.apache.kafka.common.message.LiMoveControllerRequestData;
import org.apache.kafka.common.message.LiMoveControllerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class LiMoveControllerRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiMoveControllerRequest> {
        private final LiMoveControllerRequestData data;

        public Builder(LiMoveControllerRequestData data, short allowedVersion) {
            super(ApiKeys.LI_MOVE_CONTROLLER, allowedVersion);
            this.data = data;
        }

        @Override
        public LiMoveControllerRequest build(short version) {
            return new LiMoveControllerRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiMoveControllerRequestData data;

    LiMoveControllerRequest(LiMoveControllerRequestData data, short version) {
        super(ApiKeys.LI_MOVE_CONTROLLER, version);
        this.data = data;
    }

    public static LiMoveControllerRequest parse(ByteBuffer buffer, short version) {
        return new LiMoveControllerRequest(new LiMoveControllerRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiMoveControllerResponseData data = new LiMoveControllerResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new LiMoveControllerResponse(data, version());
    }

    public LiMoveControllerRequestData data() {
        return data;
    }
}
