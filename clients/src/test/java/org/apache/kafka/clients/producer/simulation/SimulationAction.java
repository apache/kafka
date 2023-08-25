package org.apache.kafka.clients.producer.simulation;

interface SimulationAction {
    boolean maybeRun();
}
