/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.requests.ApiVersionsResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.TreeMap;

public class NodeApiVersions {
    private final Collection<ApiVersionsResponse.ApiVersion> apiVersions;

    private final static ApiKeys[] ID_TO_APIKEY;

    static {
        ID_TO_APIKEY = new ApiKeys[ApiKeys.MAX_API_KEY + 1];
        for (ApiKeys key : ApiKeys.values())
            ID_TO_APIKEY[key.id] = key;
    }

    // An array of the usable versions of each API, indexed by ApiKeys ID.
    private final short[] usableVersions;

    public NodeApiVersions(Collection<ApiVersionsResponse.ApiVersion> apiVersions) {
        this.apiVersions = apiVersions;
        this.usableVersions = new short[ID_TO_APIKEY.length];
        Arrays.fill(usableVersions, (short) -1);
        for (ApiVersionsResponse.ApiVersion apiVersion: apiVersions) {
            int index = apiVersion.apiKey;
            if ((index < 0) || (index >= usableVersions.length)) {
                continue;
            }
            if (Protocol.CURR_VERSION[index] < apiVersion.minVersion) {
                usableVersions[index] = -1;
                continue;
            }
            if (Protocol.MIN_VERSIONS[index] > apiVersion.maxVersion) {
                usableVersions[index] = -1;
                continue;
            }
            if (Protocol.CURR_VERSION[index] < apiVersion.maxVersion) {
                usableVersions[index] = Protocol.CURR_VERSION[index];
            } else {
                usableVersions[index] = apiVersion.maxVersion;
            }
        }
    }

    public short getUsableVersion(short apiKey) {
        if ((apiKey < 0) || (apiKey >= usableVersions.length)) {
            throw new UnsupportedVersionException("The client cannot send an " +
                    "API request of type " + (int) apiKey + ", because the " +
                    "server does not understand any of the versions this client supports.");
        } else {
            return usableVersions[apiKey];
        }
    }

    @Override
    public String toString() {
        // The apiVersion collection may not be in sorted order.  We put it into
        // a TreeMap before printing it out to ensure that we always print in
        // ascending order.  We also handle the case where some apiKey types are
        // unknown, which may happen when either the client or server is newer.
        //
        // This toString method is relatively expensive, so you probably want to avoid
        // calling it unless debug logging is turned on.
        TreeMap<Integer, CharSequence> apiKeysText = new TreeMap<Integer, CharSequence>();
        for (ApiVersionsResponse.ApiVersion apiVersion: this.apiVersions) {
            StringBuilder bld = new StringBuilder();
            int apiKey = apiVersion.apiKey;
            if ((apiKey < 0) || (apiKey >= ID_TO_APIKEY.length)) {
                bld.append("UNKNOWN(").append(apiKey).append("): ");
            } else {
                bld.append(ID_TO_APIKEY[apiKey].name).
                    append("(").append(apiKey).append("): ");
            }
            if (apiVersion.minVersion == apiVersion.maxVersion) {
                bld.append((int) apiVersion.minVersion);
            } else {
                bld.append((int) apiVersion.minVersion).
                        append(" to ").
                        append(apiVersion.maxVersion);
            }
            if ((apiKey >= 0) && (apiKey < ID_TO_APIKEY.length)) {
                int usableVersion = usableVersions[apiKey];
                if (usableVersion < 0) {
                    bld.append(" [usable: NONE]");
                } else {
                    bld.append(" [usable: ").append(usableVersion).append("]");
                }
            }
            apiKeysText.put(Integer.valueOf(apiVersion.apiKey), bld);
        }
        for (int apiKey = ApiKeys.MIN_API_KEY; apiKey <= ApiKeys.MAX_API_KEY; apiKey++) {
            if (!apiKeysText.containsKey(apiKey)) {
                StringBuilder bld = new StringBuilder();
                bld.append(ID_TO_APIKEY[apiKey].name).append("(").
                    append(apiKey).append("): ").append("UNSUPPORTED");
                apiKeysText.put(Integer.valueOf(apiKey), bld);
            }
        }
        StringBuilder bld = new StringBuilder();
        bld.append("{");
        String prefix = "";
        for (CharSequence seq : apiKeysText.values()) {
            bld.append(prefix).append(seq);
            prefix = ", ";
        }
        bld.append("}");
        return bld.toString();
    }
}
