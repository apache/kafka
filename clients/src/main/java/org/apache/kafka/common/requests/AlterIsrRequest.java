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

import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class AlterIsrRequest extends AbstractRequest {

    private final AlterIsrRequestData data;

    public AlterIsrRequest(AlterIsrRequestData data, short apiVersion) {
        super(ApiKeys.ALTER_ISR, apiVersion);
        this.data = data;
    }

    public AlterIsrRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    /**
     * Get an error response for a request with specified throttle time in the response if applicable
     *
     * @param throttleTimeMs
     * @param e
     */
    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AlterIsrResponse(new AlterIsrResponseData().setErrorCode(Errors.forException(e).code()));
    }
}
