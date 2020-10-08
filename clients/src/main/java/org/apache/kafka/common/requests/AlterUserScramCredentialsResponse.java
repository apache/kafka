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

import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

public class AlterUserScramCredentialsResponse extends AbstractResponse {

    private final AlterUserScramCredentialsResponseData data;

    public AlterUserScramCredentialsResponse(Struct struct) {
        this(struct, ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.latestVersion());
    }

    public AlterUserScramCredentialsResponse(AlterUserScramCredentialsResponseData responseData) {
        this.data = responseData;
    }

    AlterUserScramCredentialsResponse(Struct struct, short version) {
        this.data = new AlterUserScramCredentialsResponseData(struct, version);
    }

    public static AlterUserScramCredentialsResponse parse(ByteBuffer buffer, short version) {
        return new AlterUserScramCredentialsResponse(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.responseSchema(version).read(buffer), version);
    }

    public AlterUserScramCredentialsResponseData data() {
        return data;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(data.results().stream().map(r -> Errors.forCode(r.errorCode())));
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
