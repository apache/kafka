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
package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Objects;

import static java.util.stream.Collectors.joining;

/**
 * Represents an immutable dictionary with key being feature name, and value being <VersionRangeType>.
 * Also provides API to serialize/deserialize the features and their version ranges to/from a map.
 *
 * This class can be instantiated only using its factory functions, with the important ones being:
 * Features.supportedFeatures(...) and Features.finalizedFeatures(...).
 *
 * @param <VersionRangeType> is the type of version range.
 * @see SupportedVersionRange
 * @see FinalizedVersionRange
 */
public class Features<VersionRangeType extends BaseVersionRange> {
    private final Map<String, VersionRangeType> features;

    /**
     * Constructor is made private, as for readability it is preferred the caller uses one of the
     * static factory functions for instantiation (see below).
     *
     * @param features   Map of feature name to type of VersionRange, as the backing data structure
     *                   for the Features object.
     */
    private Features(Map<String, VersionRangeType> features) {
        if (features == null) {
            throw new IllegalArgumentException("Provided features can not be null.");
        }
        this.features = features;
    }

    /**
     * @param features   Map of feature name to SupportedVersionRange.
     *
     * @return           Returns a new Features object representing supported features.
     */
    public static Features<SupportedVersionRange> supportedFeatures(Map<String, SupportedVersionRange> features) {
        return new Features<>(features);
    }

    /**
     * @param features   Map of feature name to FinalizedVersionRange.
     *
     * @return           Returns a new Features object representing finalized features.
     */
    public static Features<FinalizedVersionRange> finalizedFeatures(Map<String, FinalizedVersionRange> features) {
        return new Features<>(features);
    }

    // Visible for testing.
    public static Features<FinalizedVersionRange> emptyFinalizedFeatures() {
        return new Features<>(new HashMap<>());
    }

    public static Features<SupportedVersionRange> emptySupportedFeatures() {
        return new Features<>(new HashMap<>());
    }

    public Map<String, VersionRangeType> features() {
        return features;
    }

    public boolean empty() {
        return features.isEmpty();
    }

    /**
     * @param  feature   name of the feature
     *
     * @return           the VersionRangeType corresponding to the feature name, or null if absent
     */
    public VersionRangeType get(String feature) {
        return features.get(feature);
    }

    public String toString() {
        return String.format(
            "Features{%s}",
            features
                .entrySet()
                .stream()
                .map(entry -> String.format("(%s -> %s)", entry.getKey(), entry.getValue()))
                .collect(joining(", "))
        );
    }

    /**
     * @return   A map with underlying features serialized. The returned value can be deserialized
     *           using one of the provided deserialize() APIs of this class.
     */
    public Map<String, Map<String, Long>> serialize() {
        return features.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().serialize()));
    }

    /**
     * An interface that defines deserialization behavior for an object of type BaseVersionRange.
     */
    private interface BaseVersionRangeDeserializer<V extends BaseVersionRange> {

        /**
         * Deserialize the serialized representation of an object of type BaseVersionRange, to an
         * object of type <V>.
         *
         * @param  serialized   the serialized representation of a BaseVersionRange object.
         *
         * @return              the deserialized object of type <V>
         */
        V deserialize(Map<String, Long> serialized);
    }

    private static <V extends BaseVersionRange> Features<V> deserializeFeatures(
        Map<String, Map<String, Long>> serialized, BaseVersionRangeDeserializer<V> deserializer) {
        return new Features<>(serialized.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> deserializer.deserialize(entry.getValue()))));
    }

    /**
     * Deserialize a map to Features<FinalizedVersionRange>.
     *
     * @param serialized   the serialized representation of a Features<FinalizedVersionRange> object,
     *                     generated using the serialize() API.
     *
     * @return             the deserialized Features<FinalizedVersionRange> object
     */
    public static Features<FinalizedVersionRange> deserializeFinalizedFeatures(
        Map<String, Map<String, Long>> serialized) {
        return deserializeFeatures(serialized, FinalizedVersionRange::deserialize);
    }

    /**
     * Deserializes a map to Features<SupportedVersionRange>.
     *
     * @param serialized   the serialized representation of a Features<SupportedVersionRange> object,
     *                     generated using the serialize() API.
     *
     * @return             the deserialized Features<SupportedVersionRange> object
     */
    public static Features<SupportedVersionRange> deserializeSupportedFeatures(
        Map<String, Map<String, Long>> serialized) {
        return deserializeFeatures(serialized, SupportedVersionRange::deserialize);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || !(other instanceof Features)) {
            return false;
        }

        final Features that = (Features) other;
        return Objects.equals(this.features, that.features);
    }

    @Override
    public int hashCode() {
        return Objects.hash(features);
    }
}
