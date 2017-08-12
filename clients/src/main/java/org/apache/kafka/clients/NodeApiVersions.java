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
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An internal class which represents the API versions supported by a particular node.
 */
public class NodeApiVersions {
    // A map of the usable versions of each API, keyed by the ApiKeys instance
    private final Map<ApiKeys, UsableVersion> usableVersions = new EnumMap<>(ApiKeys.class);

    // List of APIs which the broker supports, but which are unknown to the client
    private final List<ApiVersion> unknownApis = new ArrayList<>();

    /**
     * Create a NodeApiVersions object with the current ApiVersions.
     *
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create() {
        return create(Collections.<ApiVersion>emptyList());
    }

    /**
     * Create a NodeApiVersions object.
     *
     * @param overrides API versions to override. Any ApiVersion not specified here will be set to the current client
     *                  value.
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create(Collection<ApiVersion> overrides) {
        List<ApiVersion> apiVersions = new LinkedList<>(overrides);
        for (ApiKeys apiKey : ApiKeys.values()) {
            boolean exists = false;
            for (ApiVersion apiVersion : apiVersions) {
                if (apiVersion.apiKey == apiKey.id) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                apiVersions.add(new ApiVersion(apiKey));
            }
        }
        return new NodeApiVersions(apiVersions);
    }

    public NodeApiVersions(Collection<ApiVersion> nodeApiVersions) {
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey)) {
                ApiKeys nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey);
                usableVersions.put(nodeApiKey, new UsableVersion(nodeApiKey, nodeApiVersion));
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(nodeApiVersion);
            }
        }
    }

    /**
     * Return the most recent version supported by both the node and the local software.
     */
    public short usableVersion(ApiKeys apiKey) {
        return usableVersion(apiKey, null);
    }

    /**
     * Return the desired version (if usable) or the latest usable version if the desired version is null.
     */
    public short usableVersion(ApiKeys apiKey, Short desiredVersion) {
        UsableVersion usableVersion = usableVersions.get(apiKey);
        if (usableVersion == null)
            throw new UnsupportedVersionException("The broker does not support " + apiKey);

        if (desiredVersion == null) {
            usableVersion.ensureUsable();
            return usableVersion.value;
        } else {
            usableVersion.ensureUsable(desiredVersion);
            return desiredVersion;
        }
    }

    /**
     * Convert the object to a string with no linebreaks.<p/>
     * <p>
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
        for (UsableVersion usableVersion : this.usableVersions.values())
            apiKeysText.put(usableVersion.apiVersion.apiKey, apiVersionToText(usableVersion.apiVersion));
        for (ApiVersion apiVersion : unknownApis)
            apiKeysText.put(apiVersion.apiKey, apiVersionToText(apiVersion));

        // Also handle the case where some apiKey types are not specified at all in the given ApiVersions,
        // which may happen when the remote is too old.
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
            bld.append(apiKey.name).append("(").append(apiKey.id).append("): ");
        } else {
            bld.append("UNKNOWN(").append(apiVersion.apiKey).append("): ");
        }

        if (apiVersion.minVersion == apiVersion.maxVersion) {
            bld.append(apiVersion.minVersion);
        } else {
            bld.append(apiVersion.minVersion).append(" to ").append(apiVersion.maxVersion);
        }

        if (apiKey != null) {
            UsableVersion usableVersion = usableVersions.get(apiKey);
            if (usableVersion.isTooOld())
                bld.append(" [unusable: node too old]");
            else if (usableVersion.isTooNew())
                bld.append(" [unusable: node too new]");
            else
                bld.append(" [usable: ").append(usableVersion.value).append("]");
        }
        return bld.toString();
    }

    /**
     * Get the version information for a given API.
     *
     * @param apiKey The api key to lookup
     * @return The api version information from the broker or null if it is unsupported
     */
    public ApiVersion apiVersion(ApiKeys apiKey) {
        UsableVersion usableVersion = usableVersions.get(apiKey);
        if (usableVersion == null)
            return null;
        return usableVersion.apiVersion;
    }

    private static class UsableVersion {
        private static final short NODE_TOO_OLD = (short) -1;
        private static final short NODE_TOO_NEW = (short) -2;

        private final ApiKeys apiKey;
        private final ApiVersion apiVersion;
        private final Short value;

        private UsableVersion(ApiKeys apiKey, ApiVersion nodeApiVersion) {
            this.apiKey = apiKey;
            this.apiVersion = nodeApiVersion;
            short v = Utils.min(apiKey.latestVersion(), nodeApiVersion.maxVersion);
            if (v < nodeApiVersion.minVersion) {
                this.value = NODE_TOO_NEW;
            } else if (v < apiKey.oldestVersion()) {
                this.value = NODE_TOO_OLD;
            } else {
                this.value = v;
            }
        }

        private boolean isTooOld() {
            return value == NODE_TOO_OLD;
        }

        private boolean isTooNew() {
            return value == NODE_TOO_NEW;
        }

        private void ensureUsable() {
            if (value == NODE_TOO_OLD)
                throw new UnsupportedVersionException("The broker is too old to support " + apiKey +
                        " version " + apiKey.oldestVersion());
            else if (value == NODE_TOO_NEW)
                throw new UnsupportedVersionException("The broker is too new to support " + apiKey +
                        " version " + apiKey.latestVersion());
        }

        private void ensureUsable(short desiredVersion) {
            if (apiVersion.minVersion > desiredVersion || apiVersion.maxVersion < desiredVersion)
                throw new UnsupportedVersionException("The broker does not support the requested version " + desiredVersion +
                        " for api " + apiKey + ". Supported versions are " + apiVersion.minVersion +
                        " to " + apiVersion.maxVersion + ".");
        }

    }

}
