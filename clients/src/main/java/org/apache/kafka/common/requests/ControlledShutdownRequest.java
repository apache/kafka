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

import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;


public class ControlledShutdownRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ControlledShutdownRequest> {

        private final ControlledShutdownRequestData data;

        public Builder(ControlledShutdownRequestData data, short desiredVersion) {
            super(ApiKeys.CONTROLLED_SHUTDOWN, desiredVersion);
            this.data = data;
        }

        @Override
        public ControlledShutdownRequest build(short version) {
            return new ControlledShutdownRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ControlledShutdownRequestData data;
    private final short version;

    private ControlledShutdownRequest(ControlledShutdownRequestData data, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN, version);
        this.data = data;
        this.version = version;
    }

    public ControlledShutdownRequest(Struct struct, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN, version);
        this.data = new ControlledShutdownRequestData(struct, version);
        this.version = version;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ControlledShutdownResponseData response = new ControlledShutdownResponseData();
        response.setErrorCode(Errors.forException(e).code());
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new ControlledShutdownResponse(response);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()));
        }
    }

    public static ControlledShutdownRequest parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownRequest(
                ApiKeys.CONTROLLED_SHUTDOWN.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    public ControlledShutdownRequestData data() {
        return data;
    }
}
