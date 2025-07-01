package org.apache.kafka.storage.internals.log;

import java.util.Collections;
import java.util.List;

public interface AssignmentState {

    List<Integer> replicas();

    default int replicationFactor(){
        return replicas().size();
    }

    default Boolean isAddingReplica(int brokerId){
        return false;
    }
}

