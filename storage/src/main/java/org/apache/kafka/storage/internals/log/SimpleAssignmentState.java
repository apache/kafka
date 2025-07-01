package org.apache.kafka.storage.internals.log;

import java.util.List;

public record SimpleAssignmentState(List<Integer> replicas) implements AssignmentState{ }
