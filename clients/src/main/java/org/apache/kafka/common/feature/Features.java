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
 * Also provides API to convert the features and their version ranges to/from a map.
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
     * @param features   Map of feature name to a type of VersionRange.
     */
    private Features(Map<String, VersionRangeType> features) {
        Objects.requireNonNull(features, "Provided features can not be null.");
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
     * @return           the VersionRangeType corresponding to the feature name, or null if the
     *                   feature is absent
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
     * @return   A map representation of the underlying features. The returned value can be converted
     *           back to Features using one of the from*FeaturesMap() APIs of this class.
     */
    public Map<String, Map<String, Short>> toMap() {
        return features.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().toMap()));
    }

    /**
     * An interface that defines behavior to convert from a Map to an object of type BaseVersionRange.
     */
    private interface MapToBaseVersionRangeConverter<V extends BaseVersionRange> {

        /**
         * Convert the map representation of an object of type <V>, to an object of type <V>.
         *
         * @param  baseVersionRangeMap   the map representation of a BaseVersionRange object.
         *
         * @return                       the object of type <V>
         */
        V fromMap(Map<String, Short> baseVersionRangeMap);
    }

    private static <V extends BaseVersionRange> Features<V> fromFeaturesMap(
        Map<String, Map<String, Short>> featuresMap, MapToBaseVersionRangeConverter<V> converter) {
        return new Features<>(featuresMap.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> converter.fromMap(entry.getValue()))));
    }

    /**
     * Converts from a map to Features<FinalizedVersionRange>.
     *
     * @param featuresMap  the map representation of a Features<FinalizedVersionRange> object,
     *                     generated using the toMap() API.
     *
     * @return             the Features<FinalizedVersionRange> object
     */
    public static Features<FinalizedVersionRange> fromFinalizedFeaturesMap(
        Map<String, Map<String, Short>> featuresMap) {
        return fromFeaturesMap(featuresMap, FinalizedVersionRange::fromMap);
    }

    /**
     * Converts from a map to Features<SupportedVersionRange>.
     *
     * @param featuresMap  the map representation of a Features<SupportedVersionRange> object,
     *                     generated using the toMap() API.
     *
     * @return             the Features<SupportedVersionRange> object
     */
    public static Features<SupportedVersionRange> fromSupportedFeaturesMap(
        Map<String, Map<String, Short>> featuresMap) {
        return fromFeaturesMap(featuresMap, SupportedVersionRange::fromMap);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Features)) {
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
