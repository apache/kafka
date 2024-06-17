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
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * An internal class which represents the API versions supported by a particular node.
 */
public class NodeApiVersions {

    // A map of the usable versions of each API, keyed by the ApiKeys instance
    private final Map<ApiKeys, ApiVersion> supportedVersions = new EnumMap<>(ApiKeys.class);

    // List of APIs which the broker supports, but which are unknown to the client
    private final List<ApiVersion> unknownApis = new ArrayList<>();

    private final Map<String, SupportedVersionRange> supportedFeatures;

    private final boolean zkMigrationEnabled;

    /**
     * Create a NodeApiVersions object with the current ApiVersions.
     *
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create() {
        return create(Collections.emptyList());
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
        for (ApiKeys apiKey : ApiKeys.clientApis()) {
            boolean exists = false;
            for (ApiVersion apiVersion : apiVersions) {
                if (apiVersion.apiKey() == apiKey.id) {
                    exists = true;
                    break;
                }
            }
            if (!exists) apiVersions.add(ApiVersionsResponse.toApiVersion(apiKey));
        }
        return new NodeApiVersions(apiVersions, Collections.emptyList(), false);
    }


    /**
     * Create a NodeApiVersions object with a single ApiKey. It is mainly used in tests.
     *
     * @param apiKey ApiKey's id.
     * @param minVersion ApiKey's minimum version.
     * @param maxVersion ApiKey's maximum version.
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create(short apiKey, short minVersion, short maxVersion) {
        return create(Collections.singleton(new ApiVersion()
                .setApiKey(apiKey)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion)));
    }

    public NodeApiVersions(Collection<ApiVersion> nodeApiVersions, Collection<SupportedFeatureKey> nodeSupportedFeatures, boolean zkMigrationEnabled) {
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey())) {
                ApiKeys nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey());
                supportedVersions.put(nodeApiKey, nodeApiVersion);
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(nodeApiVersion);
            }
        }

        Map<String, SupportedVersionRange> supportedFeaturesBuilder = new HashMap<>();
        for (SupportedFeatureKey supportedFeature : nodeSupportedFeatures) {
            supportedFeaturesBuilder.put(supportedFeature.name(),
                    new SupportedVersionRange(supportedFeature.minVersion(), supportedFeature.maxVersion()));
        }
        this.supportedFeatures = Collections.unmodifiableMap(supportedFeaturesBuilder);
        this.zkMigrationEnabled = zkMigrationEnabled;
    }

    /**
     * Return the most recent version supported by both the node and the local software.
     */
    public short latestUsableVersion(ApiKeys apiKey) {
        return latestUsableVersion(apiKey, apiKey.oldestVersion(), apiKey.latestVersion());
    }

    /**
     * Get the latest version supported by the broker within an allowed range of versions
     */
    public short latestUsableVersion(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
        if (!supportedVersions.containsKey(apiKey))
            throw new UnsupportedVersionException("The node does not support " + apiKey);
        ApiVersion supportedVersion = supportedVersions.get(apiKey);
        Optional<ApiVersion> intersectVersion = ApiVersionsResponse.intersect(supportedVersion,
            new ApiVersion()
                .setApiKey(apiKey.id)
                .setMinVersion(oldestAllowedVersion)
                .setMaxVersion(latestAllowedVersion));

        if (intersectVersion.isPresent())
            return intersectVersion.get().maxVersion();
        else
            throw new UnsupportedVersionException("The node does not support " + apiKey +
                " with version in range [" + oldestAllowedVersion + "," + latestAllowedVersion + "]. The supported" +
                " range is [" + supportedVersion.minVersion() + "," + supportedVersion.maxVersion() + "].");
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
        for (ApiVersion supportedVersion : this.supportedVersions.values())
            apiKeysText.put(supportedVersion.apiKey(), apiVersionToText(supportedVersion));
        for (ApiVersion apiVersion : unknownApis)
            apiKeysText.put(apiVersion.apiKey(), apiVersionToText(apiVersion));

        // Also handle the case where some apiKey types are not specified at all in the given ApiVersions,
        // which may happen when the remote is too old.
        for (ApiKeys apiKey : ApiKeys.clientApis()) {
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
        bld.append(String.join(separator, apiKeysText.values()));
        if (lineBreaks)
            bld.append("\n");
        bld.append(")");
        return bld.toString();
    }

    private String apiVersionToText(ApiVersion apiVersion) {
        StringBuilder bld = new StringBuilder();
        ApiKeys apiKey = null;
        if (ApiKeys.hasId(apiVersion.apiKey())) {
            apiKey = ApiKeys.forId(apiVersion.apiKey());
            bld.append(apiKey.name).append("(").append(apiKey.id).append("): ");
        } else {
            bld.append("UNKNOWN(").append(apiVersion.apiKey()).append("): ");
        }

        if (apiVersion.minVersion() == apiVersion.maxVersion()) {
            bld.append(apiVersion.minVersion());
        } else {
            bld.append(apiVersion.minVersion()).append(" to ").append(apiVersion.maxVersion());
        }

        if (apiKey != null) {
            ApiVersion supportedVersion = supportedVersions.get(apiKey);
            if (apiKey.latestVersion() < supportedVersion.minVersion()) {
                bld.append(" [unusable: node too new]");
            } else if (supportedVersion.maxVersion() < apiKey.oldestVersion()) {
                bld.append(" [unusable: node too old]");
            } else {
                short latestUsableVersion = Utils.min(apiKey.latestVersion(), supportedVersion.maxVersion());
                bld.append(" [usable: ").append(latestUsableVersion).append("]");
            }
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
        return supportedVersions.get(apiKey);
    }

    public Map<ApiKeys, ApiVersion> allSupportedApiVersions() {
        return supportedVersions;
    }

    public Map<String, SupportedVersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    public boolean zkMigrationEnabled() {
        return zkMigrationEnabled;
    }
}
