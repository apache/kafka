package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * hello
 * @param assignmentEpoch 
 * @param topicIdsPerSubTopology
 */
public record TargetResolvedTopicIds(int assignmentEpoch, Map<String, List<Uuid>> topicIdsPerSubTopology) {

    /**
     * hello
     */
    public static final TargetResolvedTopicIds INITIAL = new TargetResolvedTopicIds(-1, new HashMap<>());
    
    public TargetResolvedTopicIds {
        Objects.requireNonNull(topicIdsPerSubTopology, "topicIdsPerSubTopology should not be null");

        Map<String, List<Uuid>> copy = new HashMap<>();
        topicIdsPerSubTopology.forEach((subtopologyId, topicIds) -> {
            Objects.requireNonNull(subtopologyId, "subtopologyId should not be null");
            Objects.requireNonNull(topicIds, "topicIds should not be null");
            copy.put(subtopologyId, List.copyOf(topicIds));
        });

        topicIdsPerSubTopology = Map.copyOf(copy);
    }    
}
