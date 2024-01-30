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
package org.apache.kafka.security;

import javax.crypto.spec.GCMParameterSpec;
import java.security.AlgorithmParameters;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidParameterSpecException;
import java.util.HashMap;
import java.util.Map;

public class GcmParamsEncoder implements CipherParamsEncoder {

    private static final String AUTHENTICATION_TAG_LENGTH = "authenticationTagLength";

    @Override
    public Map<String, String> toMap(AlgorithmParameters cipherParams) throws InvalidParameterSpecException {
        if (cipherParams != null) {
            GCMParameterSpec spec = cipherParams.getParameterSpec(GCMParameterSpec.class);
            Map<String, String> map = new HashMap<>();
            map.put(PasswordEncoder.INITIALIZATION_VECTOR, PasswordEncoder.base64Encode(spec.getIV()));
            map.put(AUTHENTICATION_TAG_LENGTH, String.valueOf(spec.getTLen()));
            return map;
        } else
            throw new IllegalStateException("Could not determine initialization vector for cipher");
    }

    @Override
    public AlgorithmParameterSpec toParameterSpec(Map<String, String> paramMap) {
        return new GCMParameterSpec(Integer.parseInt(paramMap.get(AUTHENTICATION_TAG_LENGTH)),
                                    PasswordEncoder.base64Decode(paramMap.get(PasswordEncoder.INITIALIZATION_VECTOR)));
    }
}
