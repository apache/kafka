package org.apache.kafka.jmh.producer;


import org.apache.kafka.common.record.KafkaLZ4BlockOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class KafkaLZ4BlockOutputStreamBenchmark {

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public KafkaLZ4BlockOutputStream constructorBenchmark() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        return new KafkaLZ4BlockOutputStream(out);
    }
}
