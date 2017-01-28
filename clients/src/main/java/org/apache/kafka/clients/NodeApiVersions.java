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
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * An internal class which represents the API versions supported by a particular node.
 */
public class NodeApiVersions {
    private static final Short API_NOT_ON_NODE = null;
    private static final short NODE_TOO_OLD = (short) -1;
    private static final short NODE_TOO_NEW = (short) -2;
    private final Collection<ApiVersion> nodeApiVersions;

    /**
     * An array of the usable versions of each API, indexed by the ApiKeys ID.
     */
    private final Map<ApiKeys, Short> usableVersions = new EnumMap<>(ApiKeys.class);

    /**
     * Create a NodeApiVersions object with the current ApiVersions.
     *
     * @return A new NodeApiVersions object.
     */
    public static NodeApiVersions create() {
        return create(Collections.EMPTY_LIST);
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
                apiVersions.add(new ApiVersion(apiKey.id, ProtoUtils.oldestVersion(apiKey.id),
                    ProtoUtils.latestVersion(apiKey.id)));
            }
        }
        return new NodeApiVersions(apiVersions);
    }

    public NodeApiVersions(Collection<ApiVersion> nodeApiVersions) {
        this.nodeApiVersions = nodeApiVersions;
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            int nodeApiKey = nodeApiVersion.apiKey;
            // Newer brokers may support ApiKeys we don't know about, ignore them
            if (ApiKeys.hasId(nodeApiKey)) {
                short v = Utils.min(ProtoUtils.latestVersion(nodeApiKey), nodeApiVersion.maxVersion);
                if (v < nodeApiVersion.minVersion) {
                    usableVersions.put(ApiKeys.forId(nodeApiKey), NODE_TOO_NEW);
                } else if (v < ProtoUtils.oldestVersion(nodeApiKey)) {
                    usableVersions.put(ApiKeys.forId(nodeApiKey), NODE_TOO_OLD);
                } else {
                    usableVersions.put(ApiKeys.forId(nodeApiKey), v);
                }
            }
        }
    }

    /**
     * Return the most recent version supported by both the node and the local software.
     */
    public short usableVersion(ApiKeys apiKey) {
        Short usableVersion = usableVersions.get(apiKey);
        if (usableVersion == API_NOT_ON_NODE)
            throw new UnsupportedVersionException("The broker does not support " + apiKey);
        else if (usableVersion == NODE_TOO_OLD)
            throw new UnsupportedVersionException("The broker is too old to support " + apiKey +
                " version " + ProtoUtils.oldestVersion(apiKey.id));
        else if (usableVersion == NODE_TOO_NEW)
            throw new UnsupportedVersionException("The broker is too new to support " + apiKey +
                " version " + ProtoUtils.latestVersion(apiKey.id));
        else
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
        for (ApiVersion apiVersion : this.nodeApiVersions)
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
            if (usableVersion == NODE_TOO_OLD)
                bld.append(" [unusable: node too old]");
            else if (usableVersion == NODE_TOO_NEW)
                bld.append(" [unusable: node too new]");
            else
                bld.append(" [usable: ").append(usableVersion).append("]");
        }
        return bld.toString();
    }

    public ApiVersion apiVersion(ApiKeys apiKey) {
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            if (nodeApiVersion.apiKey == apiKey.id) {
                return nodeApiVersion;
            }
        }
        throw new NoSuchElementException();
    }
}
