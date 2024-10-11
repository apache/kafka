package org.apache.kafka.coordinator.group.modern.consumer;

import com.google.re2j.Pattern;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegexKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegexValue;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a regular expression used in a group.
 * Includes the identifier for the regex and its resolution.
 */
public class ConsumerGroupRegex {

    /**
     * Identifier for a regex used in a group.
     */
    public static class RegexKey {
        private final String groupId;
        private final Pattern pattern;

        public static class Builder {
            private String groupId;
            private Pattern pattern;

            public Builder withGroupId(String groupId) {
                this.groupId = groupId;
                return this;
            }

            public Builder withPattern(Pattern pattern) {
                this.pattern = pattern;
                return this;
            }

            public RegexKey build() {
                return new RegexKey(this.groupId, this.pattern);
            }

            public Builder updateWith(ConsumerGroupRegexKey key) {
                this.groupId = key.groupId();
                this.pattern = Pattern.compile(key.regex());
                return this;
            }

        }

        private RegexKey(String groupId, Pattern pattern) {
            this.groupId = groupId;
            this.pattern = pattern;
        }

        public String groupId() {
            return groupId;
        }

        public Pattern pattern() {
            return pattern;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RegexKey)) return false;
            RegexKey other = (RegexKey) o;
            return pattern.equals(other.pattern) && groupId.equals(other.groupId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, groupId);
        }
    }

    /**
     * Result of evaluating a regex against topics from metadata.
     */
    public static class Resolution {
        private final Set<String> matchingTopics;
        private final int metadataVersion;
        private int memberCount;

        public static class Builder {
            private Set<String> matchingTopics = new HashSet<>();
            private int metadataVersion;
            private int memberCount;

            public Builder withMetadataVersion(int metadataVersion) {
                this.metadataVersion = metadataVersion;
                return this;
            }

            public Builder withMemberCount(int memberCount) {
                this.memberCount = memberCount;
                return this;
            }

            public Builder withMatchingTopics(Set<String> matchingTopics) {
                this.matchingTopics = matchingTopics;
                return this;
            }

            public Resolution build() {
                Resolution result = new Resolution(this.matchingTopics, this.metadataVersion);
                result.memberCount = this.memberCount;
                return result;
            }

            public Builder updateWith(ConsumerGroupRegexValue value) {
                this.matchingTopics = new HashSet<>(value.matchingTopicsNames());
                this.metadataVersion = value.metadataVersion();
                this.memberCount = value.memberCount();
                return this;
            }
        }

        private Resolution(Set<String> matchingTopics, int metadataVersion) {
            this.matchingTopics = matchingTopics;
            this.metadataVersion = metadataVersion;
        }

        /**
         * @return Set of topics names that match the regular expression.
         * This is the result of evaluating the regex against
         * the list of topics from metadata.
         */
        public Set<String> matchingTopics() {
            return this.matchingTopics;
        }

        /**
         * @return Version of the metadata used to compute the matching topics.
         */
        public int metadataVersion() {
            return this.metadataVersion;
        }

        /**
         * @return Number of members subscribed to this regular expression.
         */
        public int memberCount() {
            return this.memberCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(matchingTopics, metadataVersion);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Resolution)) return false;
            Resolution other = (Resolution) o;
            return matchingTopics.equals(other.matchingTopics) && metadataVersion == other.metadataVersion;
        }
    }
}
