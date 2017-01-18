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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.apache.kafka.common.utils.Utils;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

public class NodeApiVersions {
    private final Collection<ApiVersion> apiVersions;

    // An array of the usable versions of each API, indexed by ApiKeys ID.
    private final Map<ApiKeys, Short> usableVersions = new EnumMap<>(ApiKeys.class);

    public NodeApiVersions(Collection<ApiVersion> apiVersions) {
        this.apiVersions = apiVersions;
        for (ApiVersion apiVersion : apiVersions) {
            int apiKeyId = apiVersion.apiKey;
            // Newer brokers may support ApiKeys we don't know about, ignore them
            if (ApiKeys.hasId(apiKeyId)) {
                short version = Utils.min(ProtoUtils.latestVersion(apiKeyId), apiVersion.maxVersion);
                if (version >= apiVersion.minVersion && version >= ProtoUtils.oldestVersion(apiKeyId))
                    usableVersions.put(ApiKeys.forId(apiKeyId), version);
            }
        }
    }

    /**
     * Return the most recent version supported by both the client and the server.
     */
    public short usableVersion(ApiKeys apiKey) {
        Short usableVersion = usableVersions.get(apiKey);
        if (usableVersion == null) {
            throw new UnsupportedVersionException("The client cannot send an " +
                    "API request of type " + apiKey + ", because the " +
                    "server does not understand any of the versions this client supports.");
        }
        return usableVersion;
    }

    /**
     * Convert the object to a string with no linebreaks.<p/>
     *
     * This toString method is relatively expensive, so avoid calling it unless debug logging is turned on.
     */
    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * Convert the object to a string.
     *
     * @param lineBreaks True if we should add a linebreak after each api.
     */
    public String toString(boolean lineBreaks) {
        // The apiVersion collection may not be in sorted order.  We put it into
        // a TreeMap before printing it out to ensure that we always print in
        // ascending order.
        TreeMap<Short, String> apiKeysText = new TreeMap<>();
        for (ApiVersion apiVersion : this.apiVersions)
            apiKeysText.put(apiVersion.apiKey, apiVersionToText(apiVersion));

        // Also handle the case where some apiKey types are
        // unknown, which may happen when either the client or server is newer.
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (!apiKeysText.containsKey(apiKey.id)) {
                StringBuilder bld = new StringBuilder();
                bld.append(apiKey.name).append("(").
                        append(apiKey.id).append("): ").append("UNSUPPORTED");
                apiKeysText.put(apiKey.id, bld.toString());
            }
        }
        String separator = lineBreaks ? ",\n\t" : ", ";
        StringBuilder bld = new StringBuilder();
        bld.append("(");
        if (lineBreaks)
            bld.append("\n\t");
        bld.append(Utils.join(apiKeysText.values(), separator));
        if (lineBreaks)
            bld.append("\n");
        bld.append(")");
        return bld.toString();
    }

    private String apiVersionToText(ApiVersion apiVersion) {
        StringBuilder bld = new StringBuilder();
        ApiKeys apiKey = null;
        if (ApiKeys.hasId(apiVersion.apiKey)) {
            apiKey = ApiKeys.forId(apiVersion.apiKey);
        }
        if (apiKey != null) {
            bld.append(apiKey.name).append("(").append(apiKey.id).append("): ");
        } else {
            bld.append("UNKNOWN(").append(apiKey.id).append("): ");
        }
        if (apiVersion.minVersion == apiVersion.maxVersion) {
            bld.append(apiVersion.minVersion);
        } else {
            bld.append(apiVersion.minVersion).append(" to ").append(apiVersion.maxVersion);
        }
        if (apiKey != null) {
            Short usableVersion = usableVersions.get(apiKey);
            if (usableVersion == null) {
                bld.append(" [usable: NONE]");
            } else {
                bld.append(" [usable: ").append(usableVersion).append("]");
            }
        }
        return bld.toString();
    }

    public ApiVersion apiVersion(ApiKeys apiKey) {
        for (ApiVersion apiVersion : apiVersions) {
            if (apiVersion.apiKey == apiKey.id) {
                return apiVersion;
            }
        }
        throw new NoSuchElementException();
    }
}
