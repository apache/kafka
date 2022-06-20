package org.apache.kafka.queue;

public final class MockEventQueueMetrics implements EventQueueMetrics {
    private volatile int size = 0;

    @Override
    public void setQueueSize(int size) {
        this.size = size;
    }

    @Override
    public int eventQueueSize() {
        return this.size;
    }
}
