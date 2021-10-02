package org.apache.kafka.jmh.record;

import org.apache.kafka.common.record.CompressionConfig;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class GzipCompressedRecordBatchValidationBenchmark extends AbstractCompressedRecordBatchValidationBenchmark {

    @Param(value = {"1", "5", "9"})
    private int level = 5;

    @Override
    CompressionConfig compressionConfig() {
        return CompressionConfig.gzip().setLevel(level).build();
    }
}
