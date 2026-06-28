package org.edgeml.broker.state;

import net.openhft.chronicle.map.ChronicleMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AgentStateStore {
    private static final Logger log = LoggerFactory.getLogger(AgentStateStore.class);
    private ChronicleMap<String, String> stateMap;

    public AgentStateStore() {
        try {
            File file = new File("agent-state.dat");
            stateMap = ChronicleMap
                .of(String.class, String.class)
                .name("agent-state-map")
                .entries(10000)
                .averageKeySize(32)
                .averageValueSize(1024)
                .createPersistedTo(file);
            log.info("Initialized off-heap ChronicleMap for Agent State");
        } catch (IOException e) {
            log.error("Failed to initialize ChronicleMap", e);
        }
    }

    public void putState(String agentId, String state) {
        if (stateMap != null) {
            stateMap.put(agentId, state);
        }
    }

    public String getState(String agentId) {
        if (stateMap != null) {
            return stateMap.get(agentId);
        }
        return null;
    }
}
