package kafka.common.metrics;

import kafka.common.metrics.stats.Avg;
import kafka.common.metrics.stats.Total;

import org.junit.Test;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        Metrics metrics = new Metrics();
        metrics.addReporter(new JmxReporter());
        Sensor sensor = metrics.sensor("kafka.requests");
        sensor.add("pack.bean1.avg", new Avg());
        sensor.add("pack.bean2.total", new Total());
        Sensor sensor2 = metrics.sensor("kafka.blah");
        sensor2.add("pack.bean1.some", new Total());
        sensor2.add("pack.bean2.some", new Total());
    }
}
