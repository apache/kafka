package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.integration.SuppressionDurabilityIntegrationTest;
import org.apache.kafka.streams.integration.SuppressionIntegrationTest;
import org.apache.kafka.streams.kstream.SuppressedTest;
import org.apache.kafka.streams.kstream.internals.SuppressScenarioTest;
import org.apache.kafka.streams.kstream.internals.SuppressTopologyTest;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBufferTest;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    KTableSuppressProcessorMetricsTest.class,
    KTableSuppressProcessorTest.class,
    SuppressScenarioTest.class,
    SuppressTopologyTest.class,
    SuppressedTest.class,
    SuppressionIntegrationTest.class,
    SuppressionDurabilityIntegrationTest.class,
    InMemoryTimeOrderedKeyValueBufferTest.class,
    TimeOrderedKeyValueBufferTest.class
})
public class SuppressSuite {
}
