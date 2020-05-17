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

import java.util.Objects;

public class ClientInformation {
    public static final String UNKNOWN_NAME_OR_VERSION = "unknown";
    public static final ClientInformation EMPTY = new ClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION);

    private final String softwareName;
    private final String softwareVersion;

    public ClientInformation(String softwareName, String softwareVersion) {
        this.softwareName = softwareName.isEmpty() ? UNKNOWN_NAME_OR_VERSION : softwareName;
        this.softwareVersion = softwareVersion.isEmpty() ? UNKNOWN_NAME_OR_VERSION : softwareVersion;
    }

    public String softwareName() {
        return this.softwareName;
    }

    public String softwareVersion() {
        return this.softwareVersion;
    }

    @Override
    public String toString() {
        return "ClientInformation(softwareName=" + softwareName +
            ", softwareVersion=" + softwareVersion + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(softwareName, softwareVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof ClientInformation)) {
            return false;
        }
        ClientInformation other = (ClientInformation) o;
        return other.softwareName.equals(softwareName) &&
            other.softwareVersion.equals(softwareVersion);
    }
}
