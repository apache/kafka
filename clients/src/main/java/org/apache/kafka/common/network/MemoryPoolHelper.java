package org.apache.kafka.common.network;

public interface MemoryPoolHelper {
    
    public MemoryPoolHelper SERVER_MODE = new MemoryPoolHelper() {
        @Override
        public boolean usePool(String nodeId) {
            return true;
        }
    };

    boolean usePool(String nodeId);
}
