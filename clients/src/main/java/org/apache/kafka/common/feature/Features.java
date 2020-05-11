package org.apache.kafka.common.feature;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Objects;

import static java.util.stream.Collectors.joining;

/**
 * Represents an immutable dictionary with key being feature name, and value being VersionRangeType.
 * Also provides API to serialize/deserialize the features and their version ranges to/from a map.
 *
 * This class can be instantiated only using its factory functions, with the important ones being:
 * Features.supportedFeatures(...) and Features.finalizedFeatures(...).
 *
 * @param <VersionRangeType> is the type of version range.
 */
public class Features<VersionRangeType extends VersionRange> {
    private final Map<String, VersionRangeType> features;

    /**
     * Constructor is made private, as for readability it is preferred the caller uses one of the
     * static factory functions for instantiation (see below).
     *
     * @param features   Map of feature name to type of VersionRange, as the backing data structure
     *                   for the Features object.
     */
    private Features(Map<String, VersionRangeType> features) {
        this.features = features;
    }

    /**
     * @param features   Map of feature name to VersionRange, as the backing data structure
     *                   for the Features object.
     * @return           Returns a new Features object representing "supported" features.
     */
    public static Features<VersionRange> supportedFeatures(Map<String, VersionRange> features) {
        return new Features<VersionRange>(features);
    }

    /**
     * @param features   Map of feature name to VersionLevelRange, as the backing data structure
     *                   for the Features object.
     * @return           Returns a new Features object representing "finalized" features.
     */
    public static Features<VersionLevelRange> finalizedFeatures(Map<String, VersionLevelRange> features) {
        return new Features<VersionLevelRange>(features);
    }

    public static Features<VersionLevelRange> emptyFinalizedFeatures() {
        return new Features<>(new HashMap<>());
    }

    public static Features<VersionRange> emptySupportedFeatures() {
        return new Features<>(new HashMap<>());
    }


    public Map<String, VersionRangeType> all() {
        return features;
    }

    public boolean empty() {
        return features.isEmpty();
    }

    public VersionRangeType get(String feature) {
        return all().get(feature);
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
     * @return   Serializes the underlying features to a map, and returns the same.
     *           The returned value can be deserialized using one of the deserialize* APIs.
     */
    public Map<String, Map<String, Long>> serialize() {
        return features.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().serialize()));
    }

    /**
     * Deserializes a map to Features<VersionLevelRange>.
     *
     * @param serialized   the serialized representation of a Features<VersionLevelRange> object,
     *                     generated using the serialize() API.
     *
     * @return             the deserialized Features<VersionLevelRange> object
     */
    public static Features<VersionLevelRange> deserializeFinalizedFeatures(
        Map<String, Map<String, Long>> serialized) {
        return finalizedFeatures(serialized.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> VersionLevelRange.deserialize(entry.getValue()))));
    }

    /**
     * Deserializes a map to Features<VersionRange>.
     *
     * @param serialized   the serialized representation of a Features<VersionRange> object,
     *                     generated using the serialize() API.
     *
     * @return             the deserialized Features<VersionRange> object
     */
    public static Features<VersionRange> deserializeSupportedFeatures(
        Map<String, Map<String, Long>> serialized) {
        return supportedFeatures(serialized.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> VersionRange.deserialize(entry.getValue()))));
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
