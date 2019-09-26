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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class ConnectionMetadata {
    static final String CLIENT_ID_KEY = "ClientId";
    static final String CLIENT_SOFTWARE_NAME_KEY = "ClientSoftwareName";
    static final String CLIENT_SOFTWARE_VERSION_KEY = "ClientSoftwareVersion";
    static final String LISTENER_NAME_KEY = "ListenerName";
    static final String SECURITY_PROTOCOL_KEY = "SecurityProtocol";
    static final String CLIENT_ADDRESS_KEY = "ClientAddress";
    static final String PRINCIPAL_KEY = "Principal";

    public static final String UNKNOWN_NAME_OR_VERSION = "Unknown";

    private String clientId;
    private ListenerName listenerName;
    private SecurityProtocol securityProtocol;
    private InetAddress clientAddress;
    private KafkaPrincipal principal;

    /* They could be updated by the ConnectionRegistry */
    String clientSoftwareName;
    String clientSoftwareVersion;

    ConnectionMetadata(
        String clientId,
        String clientSoftwareName,
        String clientSoftwareVersion,
        ListenerName listenerName,
        SecurityProtocol securityProtocol,
        InetAddress clientAddress,
        KafkaPrincipal principal) {

        this.clientId = clientId;
        this.clientSoftwareName = clientSoftwareName;
        this.clientSoftwareVersion = clientSoftwareVersion;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.clientAddress = clientAddress;
        this.principal = principal;
    }

    public String clientId() {
        return this.clientId;
    }

    public String clientSoftwareName() {
        if (this.clientSoftwareName == null || this.clientSoftwareName.isEmpty())
            return UNKNOWN_NAME_OR_VERSION;
        else
            return this.clientSoftwareName;
    }

    public String clientSoftwareVersion() {
        if (this.clientSoftwareVersion == null || this.clientSoftwareVersion.isEmpty())
            return UNKNOWN_NAME_OR_VERSION;
        else
            return this.clientSoftwareVersion;
    }

    public ListenerName listenerName() {
        return this.listenerName;
    }

    public SecurityProtocol securityProtocol() {
        return this.securityProtocol;
    }

    public InetAddress clientAddress() {
        return this.clientAddress;
    }

    public KafkaPrincipal principal() {
        return this.principal;
    }

    public Map<String, String> asMap() {
        Map<String, String> map = new HashMap<>(7);
        map.put(CLIENT_ID_KEY, clientId());
        map.put(CLIENT_SOFTWARE_NAME_KEY, clientSoftwareName());
        map.put(CLIENT_SOFTWARE_VERSION_KEY, clientSoftwareVersion());
        map.put(LISTENER_NAME_KEY, listenerName().value());
        map.put(SECURITY_PROTOCOL_KEY, securityProtocol().name);
        map.put(CLIENT_ADDRESS_KEY, clientAddress().getHostAddress());
        map.put(PRINCIPAL_KEY, principal().getName());
        return map;
    }
}
