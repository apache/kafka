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

import java.io.Closeable;

public interface ChannelMetricsRegistry extends Closeable {
    /**
     * Register information about the SSL cipher we are using.
     * If we already registered this information, this call will be ignored.
     */
    void registerCipherInformation(CipherInformation cipherInfo);

    /**
     * Get the currently registered cipher information.
     */
    CipherInformation cipherInformation();

    /**
     * Register information about the client software we are communicating with.
     * If we already registered this information, this call will be ignored.
     */
    void registerClientInformation(ClientInformation clientInfo);

    /**
     * Get the currently registered client information.
     * @return
     */
    ClientInformation clientInformation();

    default String softwareName() {
        ClientInformation clientInfo = clientInformation();
        return clientInfo == null ? ClientInformation.UNKNOWN_NAME_OR_VERSION : clientInfo.softwareName();
    }

    default String softwareVersion() {
        ClientInformation clientInfo = clientInformation();
        return clientInfo == null ? ClientInformation.UNKNOWN_NAME_OR_VERSION : clientInfo.softwareVersion();
    }

    /**
     * Unregister everything that has been registered and close the registry.
     */
    void close();
}
