package org.apache.kafka.clients.admin;

import java.util.Objects;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.FinalizedVersionRange;
import org.apache.kafka.common.feature.SupportedVersionRange;

/**
 * Encapsulates details about finalized as well as supported features. This is particularly useful
 * to hold the result returned by the {@link Admin#describeFeatures(DescribeFeaturesOptions)} API.
 */
public class FeatureMetadata {

    private final Features<FinalizedVersionRange> finalizedFeatures;

    private final int finalizedFeaturesEpoch;

    private final Features<SupportedVersionRange> supportedFeatures;

    public FeatureMetadata(
        final Features<FinalizedVersionRange> finalizedFeatures,
        final int finalizedFeaturesEpoch,
        final Features<SupportedVersionRange> supportedFeatures
    ) {
        Objects.requireNonNull(finalizedFeatures, "Provided finalizedFeatures can not be null.");
        Objects.requireNonNull(supportedFeatures, "Provided supportedFeatures can not be null.");
        this.finalizedFeatures = finalizedFeatures;
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        this.supportedFeatures = supportedFeatures;
    }

    /**
     * A map of finalized feature versions, with key being finalized feature name and value
     * containing the min/max version levels for the finalized feature.
     */
    public Features<FinalizedVersionRange> finalizedFeatures() {
        return finalizedFeatures;
    }

    /**
     * The epoch for the finalized features.
     * Valid values are >= 0. A value < 0 means the finalized features are absent/unavailable.
     */
    public int finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }

    /**
     * A map of supported feature versions, with key being supported feature name and value
     * containing the min/max version for the supported feature.
     */
    public Features<SupportedVersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeatureMetadata)) {
            return false;
        }

        final FeatureMetadata that = (FeatureMetadata) other;
        return Objects.equals(this.finalizedFeatures, that.finalizedFeatures) &&
            Objects.equals(this.finalizedFeaturesEpoch, that.finalizedFeaturesEpoch) &&
            Objects.equals(this.supportedFeatures, that.supportedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
    }

    @Override
    public String toString() {
        return String.format(
            "FeatureMetadata{finalized:%s, finalizedFeaturesEpoch:%d, supported:%s}",
            finalizedFeatures,
            finalizedFeaturesEpoch,
            supportedFeatures);
    }
}
