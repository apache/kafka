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

package org.apache.kafka.message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The mapping from request/response versions to header versions for an RPC.
 *
 * The entries form an ascending, non-overlapping, contiguous set of version ranges that
 * covers the RPC's valid versions and ends with an open-ended range, e.g.
 * {@code { "0-1": "1", "2+": "2" }}.
 */
public final class HeaderVersions {
    public static final class Entry {
        private final Versions range;
        private final short headerVersion;

        Entry(Versions range, short headerVersion) {
            this.range = range;
            this.headerVersion = headerVersion;
        }

        public Versions range() {
            return range;
        }

        public short headerVersion() {
            return headerVersion;
        }
    }

    private final List<Entry> entries;

    private HeaderVersions(List<Entry> entries) {
        this.entries = entries;
    }

    /**
     * Parse the raw {@code headerVersions} map from a message schema.
     *
     * @param messageName   the message name, used in error messages
     * @param raw           the raw map from the schema, or null if the property is absent
     * @param validVersions the valid versions of the message
     * @return              the parsed header versions, or null if {@code raw} is null
     */
    public static HeaderVersions parse(String messageName, Map<String, String> raw, Versions validVersions) {
        if (raw == null) {
            return null;
        }
        if (raw.isEmpty()) {
            throw new RuntimeException("Message " + messageName + " specifies an empty headerVersions map.");
        }
        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            entries.add(parseEntry(messageName, entry.getKey(), entry.getValue()));
        }
        entries.sort(Comparator.comparingInt(entry -> entry.range.lowest()));
        validate(messageName, entries, validVersions);
        return new HeaderVersions(entries);
    }

    private static Entry parseEntry(String messageName, String key, String value) {
        if (key == null || key.trim().isEmpty()) {
            throw new RuntimeException("Message " + messageName +
                " specifies a blank version range in headerVersions.");
        }
        Versions range;
        try {
            range = Versions.parse(key, null);
        } catch (NumberFormatException e) {
            range = null;
        }
        if (range == null || range.empty()) {
            throw new RuntimeException("Message " + messageName +
                " specifies an invalid version range \"" + key + "\" in headerVersions.");
        }
        short headerVersion;
        try {
            headerVersion = Short.parseShort(value.trim());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Message " + messageName + " specifies an invalid header version \"" +
                value + "\" for range \"" + key + "\" in headerVersions.");
        }
        if (headerVersion < 0) {
            throw new RuntimeException("Message " + messageName + " specifies a negative header version \"" +
                value + "\" for range \"" + key + "\" in headerVersions.");
        }
        return new Entry(range, headerVersion);
    }

    private static void validate(String messageName, List<Entry> entries, Versions validVersions) {
        if (entries.get(0).range.lowest() != validVersions.lowest()) {
            throw new RuntimeException("Message " + messageName + " has headerVersions starting at version " +
                entries.get(0).range.lowest() + ", but the lowest valid version is " + validVersions.lowest() + ".");
        }
        for (int i = 1; i < entries.size(); i++) {
            int expected = entries.get(i - 1).range.highest() + 1;
            if (entries.get(i).range.lowest() != expected) {
                throw new RuntimeException("Message " + messageName + " has non-contiguous headerVersions: the " +
                    "range after " + entries.get(i - 1).range + " must start at version " + expected +
                    ", but it starts at version " + entries.get(i).range.lowest() + ".");
            }
        }
        Entry last = entries.get(entries.size() - 1);
        if (last.range.highest() != Short.MAX_VALUE) {
            throw new RuntimeException("Message " + messageName + " has headerVersions whose last range " +
                last.range + " is not open-ended; the last range must end with a plus sign.");
        }
        for (Entry entry : entries) {
            if (entry.range.lowest() > validVersions.highest()) {
                throw new RuntimeException("Message " + messageName + " has a headerVersions range " + entry.range +
                    " that starts above the highest valid version " + validVersions.highest() + ".");
            }
        }
    }

    public List<Entry> entries() {
        return entries;
    }

    /**
     * Return the header versions as an ordered map of strings, for serialization back to a schema.
     */
    public Map<String, String> toMap() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (Entry entry : entries) {
            map.put(entry.range.toString(), Short.toString(entry.headerVersion));
        }
        return map;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }
}
