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
package org.apache.kafka.common.network;

public class DefaultChannelMetadataRegistry implements ChannelMetadataRegistry {
    private CipherInformation cipherInformation;
    private ClientInformation clientInformation;

    @Override
    public void registerCipherInformation(final CipherInformation cipherInformation) {
        if (this.cipherInformation != null) {
            this.cipherInformation = cipherInformation;
        }
    }

    @Override
    public CipherInformation cipherInformation() {
        return this.cipherInformation;
    }

    @Override
    public void registerClientInformation(final ClientInformation clientInformation) {
        this.clientInformation = clientInformation;
    }

    @Override
    public ClientInformation clientInformation() {
        return this.clientInformation;
    }

    @Override
    public void close() {
        this.cipherInformation = null;
        this.clientInformation = null;
    }
}
