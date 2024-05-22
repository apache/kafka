package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Optional;
import java.util.Set;

/**
 * Implementation of the {@link MemberSubscriptionSpec} interface.
 */
public class MemberSubscriptionSpecImpl implements MemberSubscriptionSpec {
    private final String memberId;
    private final Optional<String> rackId;
    private final Set<Uuid> subscribedTopicIds;

    /**
     * Constructs a new {@code MemberSubscriptionSpecImpl}.
     *
     * @param memberId              The member Id.
     * @param rackId                The rack Id.
     * @param subscribedTopicIds    The set of subscribed topic Ids.
     */
    public MemberSubscriptionSpecImpl(String memberId, Optional<String> rackId, Set<Uuid> subscribedTopicIds) {
        this.memberId = memberId;
        this.rackId = rackId;
        this.subscribedTopicIds = subscribedTopicIds;
    }

    @Override
    public String memberId() {
        return memberId;
    }

    @Override
    public Optional<String> rackId() {
        return rackId;
    }

    @Override
    public Set<Uuid> subscribedTopicIds() {
        return subscribedTopicIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberSubscriptionSpecImpl that = (MemberSubscriptionSpecImpl) o;
        return memberId.equals(that.memberId) &&
            rackId.equals(that.rackId) &&
            subscribedTopicIds.equals(that.subscribedTopicIds);
    }

    @Override
    public int hashCode() {
        int result = memberId.hashCode();
        result = 31 * result + rackId.hashCode();
        result = 31 * result + subscribedTopicIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MemberSubscriptionSpecImpl(memberId=" + memberId +
            ", rackId=" + rackId.orElse("N/A") +
            ", subscribedTopicIds=" + subscribedTopicIds +
            ')';
    }
}
