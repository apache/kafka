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
package org.apache.kafka.common;

import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;

/**
 * A public class which represents the versions found on a node.
 */
public class NodeVersions {
    /**
     * The set of NodeVersions for this client software.
     */
    public static final NodeVersions CLIENT_VERSIONS;

    static {
        HashMap<Short, ApiVersionRange> apiVersionRanges = new HashMap<>();
        for (ApiKey api : ApiKey.VALUES) {
            apiVersionRanges.put(api.id(), api.supportedRange());
        }
        CLIENT_VERSIONS = new NodeVersions(apiVersionRanges);
    }

    /**
     * A mapping from API name to version range.
     */
    private final Map<Short, ApiVersionRange> apiVersionRanges;

    public NodeVersions(Map<Short, ApiVersionRange> apiVersionRanges) {
        this.apiVersionRanges = Collections.unmodifiableMap(apiVersionRanges);
    }

    /**
     * Get the ApiVersionRange for a specific API.
     *
     * @param id                                The API ID number.
     * @return                                  The ApiVersionRange.
     * @throws UnsupportedVersionException      If no versions of the API are supported.
     */
    public ApiVersionRange apiVersionRange(short id) {
        ApiVersionRange range = apiVersionRanges.get(id);
        if (range == null)
            throw new UnsupportedVersionException("UNSUPPORTED");
        return range;
    }

    /**
     * Get the ApiVersionRange for a specific API.
     *
     * @param api                               The API.
     * @return                                  The ApiVersionRange.
     * @throws UnsupportedVersionException      If no versions of the API are supported.
     */
    public ApiVersionRange apiVersionRange(ApiKey api) {
        return apiVersionRange(api.id());
    }

    /**
     * Return a map from API name to ApiVersionRange.
     */
    public Map<Short, ApiVersionRange> apiVersionRanges() {
        return apiVersionRanges;
    }

    /**
     * Return the highest version which is usable on this client.
     *
     * @return                                  The version.
     * @throws UnsupportedVersionException      If no versions of the API are usable on this client.
     */
    public short usableVersion(ApiKey api) {
        ApiVersionRange clientRange = CLIENT_VERSIONS.apiVersionRange(api);
        // All known ApiKey objects should be present in CLIENT_VERSIONS.
        Objects.requireNonNull(clientRange);
        ApiVersionRange nodeRange = apiVersionRange(api);
        if (nodeRange == null)
            throw new UnsupportedVersionException("unusable: unsupported");
        return nodeRange.usableVersion(clientRange);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean lineBreaks) {
        NavigableSet<Short> ids = new TreeSet<Short>();
        ids.addAll(apiVersionRanges.keySet());
        ids.addAll(CLIENT_VERSIONS.apiVersionRanges.keySet());
        StringBuilder bld = new StringBuilder();
        bld.append("(");
        String separator = lineBreaks ? ",\n\t" : ", ";
        String prefix = "";
        for (Short id : ids) {
            bld.append(prefix);
            prefix = separator;
            ApiKey api = null;
            try {
                api = ApiKey.fromId(id);
                bld.append(api.title()).append("(").append(api.id()).append("): ");
            } catch (IllegalArgumentException e) {
                bld.append("Unknown(").append(id).append("): ");
            }
            ApiVersionRange range = apiVersionRanges.get(id);
            if (range == null) {
                bld.append("UNSUPPORTED");
            } else {
                bld.append(range);
                if (api == null) {
                    bld.append(" [unusable: api too new]");
                } else {
                    try {
                        short usable = usableVersion(api);
                        bld.append(" [usable: ").append(usable).append("]");
                    } catch (UnsupportedVersionException e) {
                        bld.append(" [").append(e.getMessage()).append("]");
                    }
                }
            }
        }
        bld.append(")");
        return bld.toString();
    }
}
