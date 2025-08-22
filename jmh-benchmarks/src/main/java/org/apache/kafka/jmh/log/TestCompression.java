/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.jmh.log;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.jmh.record.BaseRecordBatchBenchmark;
import org.apache.kafka.jmh.record.SourceRecordFactory;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Benchmark and test harness for Kafka record compression.

 * Supports running single compression/decompression experiments or
 * sweeping multiple codecs and configurations in a matrix mode.
 * Results include throughput (MB/s), compressed size, and compression ratio.

 * Command-line options allow control over:
 *   - Compression codec (none, gzip, snappy, lz4, zstd)
 *   - Message size, batch size, and number of batches
 *   - Codec-specific properties (level, buffer, block, window, workers)
 *   - Data modes (random, zeros, mixed, debezium)
 *   - Number of runs and warmup iterations
 *   - Matrix sweeps over codecs, data, and preset config grids
 *   - CSV export of results
 *
 * Examples:
 *   - java org.apache.kafka.jmh.log.TestCompression --compression gzip --msg-size 1024 --batch-size 10 --batch-count 1000 --runs 5
 *   - java org.apache.kafka.jmh.log.TestCompression --matrix --matrix-algos all --matrix-data all --matrix-preset fast --runs 3
 */
public class TestCompression {

    public static final class DataMode {
        public enum Kind { RANDOM, ZEROS, MIXED, DEBEZIUM }
        public static Kind parse(String v) {
            return Kind.valueOf(v.trim().toUpperCase(Locale.ROOT));
        }
    }

    // ---------- Small record to carry one benchmark result ----------
    private record Result(
        String codec,
        String dataMode,
        String dataDetails,
        Map<String, Integer> codecConfig,
        double uncompressedAvgBytes,
        double compressedAvgBytes,
        double ratio,
        double mbpsAvg,
        double mbpsMedian,
        double mbpsBest,
        int runs, int warmup
    ) {}

    public static void main(String[] args) {
        OptionParser parser = new OptionParser(false);

        OptionSpec<String> dirOpt = parser.accepts("dir", "The directory that contains the uncompressed messages.")
            .withRequiredArg().describedAs("path").ofType(String.class);

        OptionSpec<Integer> msgSizeOpt = parser.accepts("msg-size", "The size of the random-generated message.")
            .withRequiredArg().describedAs("num_bytes").ofType(Integer.class).defaultsTo(1024);

        OptionSpec<Integer> batchSizeOpt = parser.accepts("batch-size", "The number of the messages in a batch.")
            .withRequiredArg().describedAs("num_count").ofType(Integer.class).defaultsTo(10);

        OptionSpec<Integer> batchCountOpt = parser.accepts("batch-count", "The number of the batches to test.")
            .withRequiredArg().describedAs("num_count").ofType(Integer.class).defaultsTo(1000);

        OptionSpec<String> compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
            .withRequiredArg().describedAs("codec").ofType(String.class)
            .defaultsTo(CompressionType.NONE.name);

        OptionSpec<String> compressionPropertyOpt = parser.accepts(
                "compression-property",
                "Per-codec property as key=value (repeatable). Only recognized keys are applied.")
            .withRequiredArg().describedAs("compression_prop").ofType(String.class);

        OptionSpec<Integer> runsOpt = parser.accepts("runs", "How many times to run the compression pass.")
            .withRequiredArg().describedAs("count").ofType(Integer.class).defaultsTo(1);

        OptionSpec<Integer> warmupOpt = parser.accepts("warmup", "How many initial runs to exclude from the summary.")
            .withRequiredArg().describedAs("count").ofType(Integer.class).defaultsTo(0);

        // ---------- Data generation mode ----------
        OptionSpec<String> dataModeOpt = parser.accepts("data", "Payload mode: random | zeros | mixed | debezium")
            .withRequiredArg().ofType(String.class).defaultsTo("random");

        OptionSpec<Integer> mixedRatioOpt = parser.accepts("mixed-ratio", "Percentage of zeros in mixed mode [0..100].")
            .withRequiredArg().ofType(Integer.class).defaultsTo(50);

        // ---------- Debezium options ----------
        OptionSpec<String> dbzEventOpt = parser.accepts("dbz-event", "Debezium event type: RANDOM | INSERT | UPDATE | DELETE")
            .withRequiredArg().ofType(String.class).defaultsTo("UPDATE");

        OptionSpec<Integer> dbzMaxFieldsOpt = parser.accepts("dbz-max-fields", "Maximum random fields per record (factory hint).")
            .withRequiredArg().ofType(Integer.class).defaultsTo(10);

        OptionSpec<Boolean> dbzSchemasOpt = parser.accepts("dbz-schemas", "If true, include Connect schemas in JSON.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

        // ---------- Matrix options ----------
        OptionSpec<Boolean> matrixOpt = parser.accepts("matrix", "Run a parameter sweep over codecs, data modes, and per-codec configs.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

        OptionSpec<String> matrixAlgosOpt = parser.accepts("matrix-algos", "Comma list of codecs (none,gzip,snappy,lz4,zstd) or 'all'")
            .withRequiredArg().ofType(String.class).defaultsTo("all");

        OptionSpec<String> matrixDataOpt = parser.accepts("matrix-data", "Comma list of data modes (random,zeros,mixed,debezium) or 'all'")
            .withRequiredArg().ofType(String.class).defaultsTo("all");

        OptionSpec<String> matrixPresetOpt = parser.accepts("matrix-preset", "Config breadth: fast | full")
            .withRequiredArg().ofType(String.class).defaultsTo("full");

        OptionSpec<String> csvOpt = parser.accepts("csv", "Write all results to CSV file (path).")
            .withRequiredArg().ofType(String.class);

        OptionSet options = parser.parse(args);
        CommandLineUtils.checkRequiredArgs(parser, options);

        final boolean matrix = options.valueOf(matrixOpt);
        if (!matrix) {
            // === original single-combo path (with enhanced summary) ===
            Result r = runOneCombo(
                options, dirOpt, msgSizeOpt, batchSizeOpt, batchCountOpt, compressionCodecOpt,
                compressionPropertyOpt, runsOpt, warmupOpt, dataModeOpt, mixedRatioOpt,
                dbzEventOpt, dbzMaxFieldsOpt, dbzSchemasOpt
            );
            // CSV if requested
            if (options.has(csvOpt)) writeCsvHeaderIfNeeded(Path.of(options.valueOf(csvOpt)));
            if (options.has(csvOpt)) appendCsv(Path.of(options.valueOf(csvOpt)), List.of(r));
            return;
        }

        // === MATRIX path ==
        final int batchCount = options.valueOf(batchCountOpt);
        final int batchSize = options.valueOf(batchSizeOpt);
        final int runs = Math.max(1, options.valueOf(runsOpt));
        int warmup = Math.max(0, options.valueOf(warmupOpt));
        if (warmup >= runs) warmup = Math.max(0, runs - 1);

        final Set<String> algos = parseSet(options.valueOf(matrixAlgosOpt),
            List.of("none","gzip","snappy","lz4","zstd"));
        final Set<DataMode.Kind> datas = parseDataModes(options.valueOf(matrixDataOpt));
        final String preset = options.valueOf(matrixPresetOpt).toLowerCase(Locale.ROOT);
        final List<Map<String,Integer>> gzipConfigs   = preset.equals("full") ? gzipGridFull()   : gzipGridFast();
        final List<Map<String,Integer>> snappyConfigs = preset.equals("full") ? snappyGridFull() : snappyGridFast();
        final List<Map<String,Integer>> lz4Configs    = preset.equals("full") ? lz4GridFull()    : lz4GridFast();
        final List<Map<String,Integer>> zstdConfigs   = preset.equals("full") ? zstdGridFull()   : zstdGridFast();
        final List<Map<String,Integer>> noneConfigs   = List.of(Collections.emptyMap());
        final Path csvPath = options.has(csvOpt)
            ? Path.of(options.valueOf(csvOpt))
            : Path.of("comp-matrix-" + Instant.now().toEpochMilli() + ".csv");
        writeCsvHeaderIfNeeded(csvPath);

        System.out.printf(Locale.ROOT,
            "MATRIX sweep: algos=%s, data=%s, preset=%s, runs=%d warmup=%d, batchCount=%d batchSize=%d%n",
            algos, datas, preset, runs, warmup, batchCount, batchSize
        );

        // --- progress bar setup ---
        final int totalCombos = totalMatrixCombos(algos, datas, gzipConfigs, snappyConfigs, lz4Configs, zstdConfigs, noneConfigs);
        int doneCombos = 0;
        final long startNs = System.nanoTime();
        printProgressBar(doneCombos, totalCombos, startNs, "starting…");

        List<Result> buffer = new ArrayList<>();

        for (String algo : algos) {
            List<Map<String,Integer>> grid = switch (algo) {
                case "gzip"   -> gzipConfigs;
                case "snappy" -> snappyConfigs;
                case "lz4"    -> lz4Configs;
                case "zstd"   -> zstdConfigs;
                case "none"   -> noneConfigs;
                default -> throw new IllegalArgumentException("Unknown algo: " + algo);
            };

            for (DataMode.Kind dm : datas) {
                for (Map<String,Integer> cfg : grid) {
                    // Build args for the single runner
                    List<String> argList = new ArrayList<>();
                    argList.add("--compression"); argList.add(algo);
                    argList.add("--batch-size"); argList.add(String.valueOf(batchSize));
                    argList.add("--batch-count"); argList.add(String.valueOf(batchCount));
                    argList.add("--runs"); argList.add(String.valueOf(runs));
                    argList.add("--warmup"); argList.add(String.valueOf(warmup));
                    argList.add("--data"); argList.add(dm.name().toLowerCase(Locale.ROOT));
                    if (dm == DataMode.Kind.MIXED) {
                        argList.add("--mixed-ratio"); argList.add("50"); // default mix
                    }
                    // per-codec properties
                    for (Map.Entry<String,Integer> e : cfg.entrySet()) {
                        argList.add("--compression-property");
                        argList.add(e.getKey() + "=" + e.getValue());
                    }

                    // Convert to OptionSet to reuse runOneCombo
                    OptionSet os = parser.parse(argList.toArray(new String[0]));
                    Result r = runOneCombo(
                        os, dirOpt, msgSizeOpt, batchSizeOpt, batchCountOpt, compressionCodecOpt,
                        compressionPropertyOpt, runsOpt, warmupOpt, dataModeOpt, mixedRatioOpt,
                        dbzEventOpt, dbzMaxFieldsOpt, dbzSchemasOpt
                    );
                    buffer.add(r);

                    // progress bar update (after each combo)
                    doneCombos++;
                    // Short label for the bar: algo | data | short cfg
                    CompressionType ct = CompressionType.forName(algo);
                    Map<String,Integer> eff = effectiveCodecConfig(ct, defaultCodecProps()); // start from defaults…
                    eff.putAll(cfg); // …and overlay current grid
                    String label = algo + " | " + dm.name().toLowerCase(Locale.ROOT) + " | " + shortCodecConfig(ct, eff);
                    printProgressBar(doneCombos, totalCombos, startNs, label);

                    // Flush every so often to keep file growing
                    if (buffer.size() >= 16) {
                        appendCsv(csvPath, buffer);
                        buffer.clear();
                    }
                }
            }
        }
        if (!buffer.isEmpty()) appendCsv(csvPath, buffer);
        System.out.println(); // move to a new line after the final bar
        System.out.println("MATRIX completed. CSV -> " + csvPath.toAbsolutePath());
    }

    // ---------- Single combo runner ----------
    private static Result runOneCombo(
        OptionSet options,
        OptionSpec<String> dirOpt,
        OptionSpec<Integer> msgSizeOpt,
        OptionSpec<Integer> batchSizeOpt,
        OptionSpec<Integer> batchCountOpt,
        OptionSpec<String> compressionCodecOpt,
        OptionSpec<String> compressionPropertyOpt,
        OptionSpec<Integer> runsOpt,
        OptionSpec<Integer> warmupOpt,
        OptionSpec<String> dataModeOpt,
        OptionSpec<Integer> mixedRatioOpt,
        OptionSpec<String> dbzEventOpt,
        OptionSpec<Integer> dbzMaxFieldsOpt,
        OptionSpec<Boolean> dbzSchemasOpt
    ) {
        final int batchCount = options.valueOf(batchCountOpt);
        final int batchSize = options.valueOf(batchSizeOpt);
        final int totalRecords = batchCount * batchSize;
        final int runs = Math.max(1, options.valueOf(runsOpt));
        int warmup = Math.max(0, options.valueOf(warmupOpt));
        if (warmup >= runs) warmup = Math.max(0, runs - 1);

        final DataMode.Kind dataMode = DataMode.parse(options.valueOf(dataModeOpt));
        final int mixedRatio = Math.min(100, Math.max(0, options.valueOf(mixedRatioOpt)));

        final String dbzEventStr = options.valueOf(dbzEventOpt);
        final int dbzMaxFields = options.valueOf(dbzMaxFieldsOpt);
        final boolean dbzSchemas = options.valueOf(dbzSchemasOpt);

        if (!options.has("matrix")) {
            System.out.printf(
                Locale.ROOT,
                "Starting TestCompression:%n" +
                    "  Compression codec : %s%n" +
                    "  Batch count       : %d%n" +
                    "  Batch size        : %d%n" +
                    "  Message size      : %s%n" +
                    "  Data source       : %s%s%n" +
                    "  Runs & Warmup     : %d & %d%n",
                options.valueOf(compressionCodecOpt),
                batchCount,
                batchSize,
                options.has(dirOpt) ? "-" : (dataMode == DataMode.Kind.DEBEZIUM ? "-" : options.valueOf(msgSizeOpt)),
                options.has(dirOpt) ? "files"
                    : dataMode == DataMode.Kind.DEBEZIUM ? "debezium(json)"
                    : dataMode.name().toLowerCase(Locale.ROOT),
                dataMode == DataMode.Kind.MIXED ? " (" + mixedRatio + "% zeros)" :
                    (dataMode == DataMode.Kind.DEBEZIUM ? String.format(" [event=%s, maxFields=%d, schemas=%s]", dbzEventStr, dbzMaxFields, dbzSchemas) : ""),
                runs, warmup
            );
        }

        // --- Build SimpleRecord payloads ---
        final List<SimpleRecord> records;
        if (options.has(dirOpt)) {
            String dir = options.valueOf(dirOpt);
            File[] files = Optional.ofNullable(new File(dir).listFiles()).orElse(new File[0]);
            List<byte[]> payloads = Arrays.stream(files)
                .filter(File::isFile)
                .limit(totalRecords)
                .map(f -> {
                    try { return Files.readAllBytes(f.toPath()); }
                    catch (Exception e) { throw new RuntimeException(e); }
                })
                .collect(Collectors.toList());
            if (payloads.size() < totalRecords && !options.has("matrix")) {
                System.out.printf("Warning: only %d files found; fewer records will be used.%n", payloads.size());
            }
            records = payloads.stream().map(a -> new SimpleRecord(0L, null, a)).collect(Collectors.toList());
        } else if (dataMode == DataMode.Kind.DEBEZIUM) {
            final SourceRecordFactory factory = new SourceRecordFactory(dbzMaxFields);
            final org.apache.kafka.connect.json.JsonConverter json = new org.apache.kafka.connect.json.JsonConverter();
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("schemas.enable", Boolean.toString(dbzSchemas));
            json.configure(cfg, false);

            final BaseRecordBatchBenchmark.DbzEvent event =
                BaseRecordBatchBenchmark.DbzEvent.valueOf(dbzEventStr.toUpperCase(Locale.ROOT));
            records = new ArrayList<>(totalRecords);
            for (int i = 0; i < totalRecords; i++) {
                SourceRecord sr = factory.createSourceRecord(
                    event == BaseRecordBatchBenchmark.DbzEvent.RANDOM ? BaseRecordBatchBenchmark.DbzEvent.RANDOM : event);
                byte[] val = json.fromConnectData(sr.topic(), sr.valueSchema(), sr.value());
                records.add(new SimpleRecord(0L, null, val));
            }
        } else {
            final int msgSize = options.valueOf(msgSizeOpt);
            records = new ArrayList<>(totalRecords);
            switch (dataMode) {
                case ZEROS -> {
                    byte[] zero = new byte[msgSize];
                    for (int i = 0; i < totalRecords; i++) records.add(new SimpleRecord(0L, null, zero));
                }
                case MIXED -> {
                    final Random rand = new Random(12345L);
                    final int zerosTarget = (int) Math.round(totalRecords * (mixedRatio / 100.0));
                    int zerosSoFar = 0;
                    for (int i = 0; i < totalRecords; i++) {
                        if (zerosSoFar < zerosTarget) {
                            records.add(new SimpleRecord(0L, null, new byte[msgSize]));
                            zerosSoFar++;
                        } else {
                            byte[] a = new byte[msgSize];
                            rand.nextBytes(a);
                            records.add(new SimpleRecord(0L, null, a));
                        }
                    }
                    Collections.shuffle(records, new Random(999L));
                }
                case RANDOM -> {
                    final Random rand = new Random(12345L);
                    for (int i = 0; i < totalRecords; i++) {
                        byte[] a = new byte[msgSize];
                        rand.nextBytes(a);
                        records.add(new SimpleRecord(0L, null, a));
                    }
                }
            }
        }

        // Partition into batches
        final List<List<SimpleRecord>> batches = new ArrayList<>(batchCount);
        for (int i = 0; i + batchSize <= records.size() && batches.size() < batchCount; i += batchSize) {
            batches.add(records.subList(i, i + batchSize));
        }
        if (batches.isEmpty()) throw new IllegalStateException("No batches to process.");

        // Compression config
        final CompressionType compressionType = CompressionType.forName(options.valueOf(compressionCodecOpt));
        final Map<String, Integer> base = defaultCodecProps();
        Properties kvProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(compressionPropertyOpt));
        Map<String, Integer> givenProps = kvProps.entrySet().stream()
            .map(e -> Map.entry(e.getKey().toString(), e.getValue().toString()))
            .filter(e -> base.containsKey(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue())));
        Map<String, Integer> compressionProps = new HashMap<>(base);
        compressionProps.putAll(givenProps);
        // Effective config for the selected codec
        Map<String,Integer> effectiveCfg = effectiveCodecConfig(compressionType, compressionProps);

        System.out.printf("  Codec config      : %s%n", humanReadableCodecConfig(compressionType, effectiveCfg));

        Compression.Builder<? extends Compression> cb = switch (compressionType) {
            case GZIP -> Compression.gzip()
                .level(compressionProps.get(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                .bufferSize(compressionProps.get(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG));
            case SNAPPY -> Compression.snappy()
                .blockSize(compressionProps.get(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG));
            case LZ4 -> Compression.lz4()
                .level(compressionProps.get(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                .blockSize(compressionProps.get(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG));
            case ZSTD -> Compression.zstd()
                .level(compressionProps.get(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                .windowSize(compressionProps.get(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG))
                .workers(compressionProps.get(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG));
            default -> Compression.none();
        };
        final Compression compression = cb.build();

        // Uncompressed baseline
        double uncompressedSum = 0.0;
        for (List<SimpleRecord> batch : batches) {
            ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(batch));
            MemoryRecordsBuilder b = MemoryRecords.builder(buf, Compression.NONE, TimestampType.CREATE_TIME, 0L);
            for (SimpleRecord r : batch) b.append(r);
            b.build();
            uncompressedSum += b.buffer().position();
        }
        final double uncompressedAvg = uncompressedSum / batches.size();
        final double totalUncompressedBytes = uncompressedSum;

        if (!options.has("matrix")) {
            System.out.printf(Locale.ROOT, "%6s  %12s  %22s  %22s  %12s%n",
                "run", "mb_sec", "uncompressed_avg_bytes", "compressed_avg_bytes", "ratio");
        }

        List<Double> runThroughputs = new ArrayList<>(runs);
        double compressedAvgForSummary = 0.0;

        for (int run = 1; run <= runs; run++) {
            long startNs = System.nanoTime();
            double compressedSum = 0.0;

            for (List<SimpleRecord> batch : batches) {
                ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(batch));
                MemoryRecordsBuilder b = MemoryRecords.builder(buf, compression, TimestampType.CREATE_TIME, 0L);
                for (SimpleRecord r : batch) b.append(r);
                b.build();
                compressedSum += b.buffer().position();
            }

            long endNs = System.nanoTime();
            double elapsed = (endNs - startNs) / 1_000_000_000.0;
            if (elapsed <= 0.0) elapsed = Double.MIN_VALUE;

            double mb = totalUncompressedBytes / (1024.0 * 1024.0);
            double mbps = mb / elapsed;

            double compressedAvg = compressedSum / batches.size();
            compressedAvgForSummary = compressedAvg;

            double ratio = compressedAvg / uncompressedAvg;

            if (!options.has("matrix")) {
                System.out.printf(Locale.ROOT, "%6d  %12.3f  %22.1f  %22.1f  %12.3f%n",
                    run, mbps, uncompressedAvg, compressedAvg, ratio);
            } else {
                System.out.printf(Locale.ROOT, "[%s|%s|%s] run=%d mbps=%.2f ratio=%.3f%n",
                    options.valueOf(compressionCodecOpt),
                    options.valueOf(dataModeOpt),
                    shortCodecConfig(compressionType, effectiveCfg),
                    run, mbps, ratio);
            }
            runThroughputs.add(mbps);
        }

        double avg, best, median;
        if (runs > 1) {
            List<Double> summary = runThroughputs.subList(Math.min(warmup, runThroughputs.size()), runThroughputs.size());
            avg = summary.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            best = summary.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
            median = median(summary);
            if (!options.has("matrix")) {
                System.out.printf(Locale.ROOT,
                    "SUMMARY (runs=%d, warmup=%d): avg=%.3f MB/s, median=%.3f MB/s, best=%.3f MB/s%n",
                    summary.size(), warmup, avg, median, best);
            }
        } else {
            avg = best = median = runThroughputs.get(0);
        }

        if (!options.has("matrix")) {
            double ratio = compressedAvgForSummary / uncompressedAvg;
            double savingsPct = (1.0 - ratio) * 100.0;
            System.out.printf(Locale.ROOT, "Uncompressed Size (avg): %.1f bytes.%n", uncompressedAvg);
            System.out.printf(Locale.ROOT, "Compressed Size (avg)  : %.1f bytes.%n", compressedAvgForSummary);
            System.out.printf(Locale.ROOT, "Compression Ratio      : %.3f (%.1f%% smaller)%n", ratio, savingsPct);
        }

        // Produce a stable config map for CSV (only keys that apply to this codec)
        Map<String,Integer> codecCfg = extractUsedCodecProps(options, compressionCodecOpt, compressionPropertyOpt);

        return new Result(
            options.valueOf(compressionCodecOpt).toLowerCase(Locale.ROOT),
            options.valueOf(dataModeOpt).toLowerCase(Locale.ROOT),
            dataModeDetails(options, dataMode, mixedRatio, dbzEventStr, dbzMaxFields, dbzSchemas),
            codecCfg,
            uncompressedAvg,
            compressedAvgForSummary,
            compressedAvgForSummary / uncompressedAvg,
            avg, median, best,
            runs, warmup
        );
    }

    // -- helpers --
    private static String dataModeDetails(OptionSet os, DataMode.Kind dm, int mixedRatio,
                                          String dbzEventStr, int dbzMaxFields, boolean dbzSchemas) {
        if (dm == DataMode.Kind.MIXED) return "mixed_ratio=" + mixedRatio;
        if (dm == DataMode.Kind.DEBEZIUM) return "event=" + dbzEventStr + ";maxFields=" + dbzMaxFields + ";schemas=" + dbzSchemas;
        return "";
    }

    private static Map<String,Integer> defaultCodecProps() {
        Map<String,Integer> m = new HashMap<>();
        m.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG,  org.apache.kafka.common.record.CompressionType.GZIP.defaultLevel());
        m.put(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, org.apache.kafka.common.record.CompressionType.GZIP.defaultBuffer());
        m.put(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG, org.apache.kafka.common.record.CompressionType.GZIP.defaultStrategy());
        m.put(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, org.apache.kafka.common.record.CompressionType.SNAPPY.defaultBlockSize());
        m.put(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG,   org.apache.kafka.common.record.CompressionType.LZ4.defaultLevel());
        m.put(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG,   org.apache.kafka.common.record.CompressionType.LZ4.defaultBlockSize());
        m.put(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG,  org.apache.kafka.common.record.CompressionType.ZSTD.defaultLevel());
        m.put(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, org.apache.kafka.common.record.CompressionType.ZSTD.defaultWindowSize());
        m.put(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, org.apache.kafka.common.record.CompressionType.ZSTD.defaultWorkers());
        return m;
    }

    private static Map<String,Integer> extractUsedCodecProps(OptionSet os,
                                                             OptionSpec<String> codecOpt,
                                                             OptionSpec<String> compressionPropertyOpt) {
        String codec = os.valueOf(codecOpt).toLowerCase(Locale.ROOT);
        Properties kvProps = CommandLineUtils.parseKeyValueArgs(os.valuesOf(compressionPropertyOpt));
        Map<String,Integer> m = new LinkedHashMap<>();
        for (Map.Entry<Object,Object> e : kvProps.entrySet()) {
            String k = String.valueOf(e.getKey());
            String v = String.valueOf(e.getValue());
            if (codec.equals("gzip") && (k.startsWith("compression.gzip."))) m.put(k, Integer.parseInt(v));
            if (codec.equals("snappy") && (k.startsWith("compression.snappy."))) m.put(k, Integer.parseInt(v));
            if (codec.equals("lz4") && (k.startsWith("compression.lz4."))) m.put(k, Integer.parseInt(v));
            if (codec.equals("zstd") && (k.startsWith("compression.zstd."))) m.put(k, Integer.parseInt(v));
        }
        return m;
    }

    private static Set<String> parseSet(String csv, List<String> all) {
        if ("all".equalsIgnoreCase(csv)) return new HashSet<>(all);
        return Arrays.stream(csv.split(","))
            .map(s -> s.trim().toLowerCase(Locale.ROOT))
            .filter(all::contains)
            .collect(Collectors.toCollection(HashSet::new));
    }

    private static Set<DataMode.Kind> parseDataModes(String csv) {
        if ("all".equalsIgnoreCase(csv)) return EnumSet.allOf(DataMode.Kind.class);
        Set<DataMode.Kind> out = EnumSet.noneOf(DataMode.Kind.class);
        for (String s : csv.split(",")) out.add(DataMode.parse(s));
        return out;
    }

    private static double median(List<Double> vals) {
        if (vals.isEmpty()) return 0.0;
        List<Double> copy = new ArrayList<>(vals);
        copy.sort(Double::compareTo);
        int n = copy.size();
        return (n % 2 == 1) ? copy.get(n / 2) : (copy.get(n / 2 - 1) + copy.get(n / 2)) / 2.0;
    }

    // ---------- CSV I/O ----------
    private static void writeCsvHeaderIfNeeded(Path path) {
        if (Files.exists(path)) return;
        String header = String.join(",",
            "codec","data_mode","data_details",
            "gzip.level","gzip.buffer", "gzip.strategy",
            "snappy.block",
            "lz4.level","lz4.block",
            "zstd.level","zstd.window","zstd.workers",
            "uncompressed_avg_bytes","compressed_avg_bytes","ratio",
            "mbps_avg","mbps_median","mbps_best",
            "runs","warmup"
        ) + "\n";
        try {
            Files.createDirectories(path.getParent() == null ? Path.of(".") : path.getParent());
            Files.write(path, header.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void appendCsv(Path path, List<Result> results) {
        StringBuilder sb = new StringBuilder();
        for (Result r : results) {
            Map<String,Integer> cfg = r.codecConfig();
            sb.append(escape(r.codec())).append(',')
                .append(escape(r.dataMode())).append(',')
                .append(escape(r.dataDetails())).append(',')

                .append(cfg.getOrDefault("compression.gzip.level",  CompressionType.GZIP.defaultLevel())).append(',')
                .append(cfg.getOrDefault("compression.gzip.buffer", CompressionType.GZIP.defaultBuffer())).append(',')
                .append(cfg.getOrDefault("compression.gzip.strategy", CompressionType.GZIP.defaultStrategy())).append(',')

                .append(cfg.getOrDefault("compression.snappy.block", CompressionType.SNAPPY.defaultBlockSize())).append(',')

                .append(cfg.getOrDefault("compression.lz4.level", CompressionType.LZ4.defaultLevel())).append(',')
                .append(cfg.getOrDefault("compression.lz4.block", CompressionType.LZ4.defaultBlockSize())).append(',')

                .append(cfg.getOrDefault("compression.zstd.level", CompressionType.ZSTD.defaultLevel())).append(',')
                .append(cfg.getOrDefault("compression.zstd.window", CompressionType.ZSTD.defaultWindowSize())).append(',')
                .append(cfg.getOrDefault("compression.zstd.workers", CompressionType.ZSTD.defaultWorkers())).append(',')

                .append(String.format(Locale.ROOT, "%.1f", r.uncompressedAvgBytes())).append(',')
                .append(String.format(Locale.ROOT, "%.1f", r.compressedAvgBytes())).append(',')
                .append(String.format(Locale.ROOT, "%.6f", r.ratio())).append(',')
                .append(String.format(Locale.ROOT, "%.3f", r.mbpsAvg())).append(',')
                .append(String.format(Locale.ROOT, "%.3f", r.mbpsMedian())).append(',')
                .append(String.format(Locale.ROOT, "%.3f", r.mbpsBest())).append(',')
                .append(r.runs()).append(',')
                .append(r.warmup()).append('\n');
        }
        try {
            Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8), java.nio.file.StandardOpenOption.APPEND, java.nio.file.StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Returns only the properties that matter for the chosen codec (defaults + overrides already merged)
    private static Map<String, Integer> effectiveCodecConfig(CompressionType ct, Map<String, Integer> props) {
        Map<String, Integer> out = new LinkedHashMap<>();
        switch (ct) {
            case GZIP -> {
                out.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG,  props.get(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG));
                out.put(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, props.get(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG));
                out.put(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG, props.get(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG));
            }
            case SNAPPY -> {
                out.put(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, props.get(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG));
            }
            case LZ4 -> {
                out.put(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, props.get(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG));
                out.put(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG, props.get(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG));
            }
            case ZSTD -> {
                out.put(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG,   props.get(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG));
                out.put(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG,  props.get(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG));
                out.put(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, props.get(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG));
            }
            case NONE -> { /* nothing */ }
        }
        return out;
    }

    // Nice single-line for the banner (long form)
    private static String humanReadableCodecConfig(CompressionType ct, Map<String,Integer> cfg) {
        return switch (ct) {
            case GZIP  -> String.format("gzip.level=%d, gzip.buffer=%d, gzip.strategy=%d",
                cfg.get(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG));
            case SNAPPY-> String.format("snappy.block=%d",
                cfg.get(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG));
            case LZ4   -> String.format("lz4.level=%d, lz4.block=%d",
                cfg.get(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG));
            case ZSTD  -> String.format("zstd.level=%d, zstd.window=%d, zstd.workers=%d",
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG));
            case NONE  -> "-";
        };
    }


    // Short form for matrix log line
    private static String shortCodecConfig(CompressionType ct, Map<String,Integer> cfg) {
        return switch (ct) {
            case GZIP  -> String.format("l=%d,buf=%d",
                cfg.get(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG));
            case SNAPPY-> String.format("blk=%d",
                cfg.get(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG));
            case LZ4   -> String.format("l=%d,blk=%d",
                cfg.get(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG));
            case ZSTD  -> String.format("l=%d,w=%d,wrk=%d",
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG),
                cfg.get(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG));
            case NONE  -> "-";
        };
    }

    private static String escape(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"")) {
            return "\"" + s.replace("\"","\"\"") + "\"";
        }
        return s;
    }

    // ---------- Config grids (fast/full) ----------
    private static List<Map<String,Integer>> gzipGridFast() {
        return List.of(
            Map.of(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 1, ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, 8192),
            Map.of(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 6, ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, 32768),
            Map.of(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 9, ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, 32768)
        );
    }

    private static List<Map<String, Integer>> gzipGridFull() {
        List<Integer> levels     = List.of(-1, 3, 6, 9);
        List<Integer> buffers    = List.of(8192, 32768, 131072);
        List<Integer> strategies = List.of(0, 1, 2); // 0=DEFAULT, 1=FILTERED, 2=HUFFMAN_ONLY

        List<Map<String, Integer>> out = new ArrayList<>();
        for (int l : levels)
            for (int b : buffers)
                for (int s : strategies) {
                    Map<String, Integer> m = new LinkedHashMap<>();
                    m.put(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, l);
                    m.put(ProducerConfig.COMPRESSION_GZIP_BUFFER_CONFIG, b);
                    m.put(ProducerConfig.COMPRESSION_GZIP_STRATEGY_CONFIG, s);
                    out.add(m);
                }
        return out;
    }

    private static List<Map<String,Integer>> snappyGridFast() {
        return List.of(
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 16384),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 32768),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 65536)
        );
    }

    private static List<Map<String,Integer>> snappyGridFull() {
        return List.of(
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 8192),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 16384),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 32768),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 65536),
            Map.of(ProducerConfig.COMPRESSION_SNAPPY_BLOCK_CONFIG, 131072)
        );
    }

    private static List<Map<String,Integer>> lz4GridFast() {
        return List.of(
            Map.of(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, 1,  ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG, 4),
            Map.of(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, 9,  ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG, 6),
            Map.of(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, 17, ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG, 7)
        );
    }

    private static List<Map<String,Integer>> lz4GridFull() {
        List<Integer> levels = List.of(1, 6, 9, 12, 17);
        List<Integer> blocks = List.of(4, 5, 6, 7);
        List<Map<String,Integer>> out = new ArrayList<>();
        for (int l : levels) for (int b : blocks)
            out.add(Map.of(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, l, ProducerConfig.COMPRESSION_LZ4_BLOCK_CONFIG, b));
        return out;
    }

    private static List<Map<String,Integer>> zstdGridFast() {
        return List.of(
            Map.of(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 1, ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, 0,  ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, 0),
            Map.of(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 3, ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, 20, ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, 0),
            Map.of(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 6, ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, 24, ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, 4),
            Map.of(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 9, ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, 24, ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, 8)
        );
    }

    private static List<Map<String,Integer>> zstdGridFull() {
        List<Integer> levels = List.of(1, 3, 6, 9, 22);
        List<Integer> windows = List.of(0, 10, 27);
        List<Integer> workers = List.of(0, 4);
        List<Map<String,Integer>> out = new ArrayList<>();
        for (int l : levels) for (int w : windows) for (int wk : workers) {
            Map<String,Integer> m = new LinkedHashMap<>();
            m.put(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, l);
            m.put(ProducerConfig.COMPRESSION_ZSTD_WINDOW_CONFIG, w);
            m.put(ProducerConfig.COMPRESSION_ZSTD_WORKERS_CONFIG, wk);
            out.add(m);
        }
        return out;
    }

    // --- progress bar stuff helpers
    private static int totalMatrixCombos(
        Set<String> algos,
        Set<DataMode.Kind> datas,
        List<Map<String,Integer>> gzipConfigs,
        List<Map<String,Integer>> snappyConfigs,
        List<Map<String,Integer>> lz4Configs,
        List<Map<String,Integer>> zstdConfigs,
        List<Map<String,Integer>> noneConfigs
    ) {
        int cfgs = algos.stream()
            .mapToInt(a -> configCountForAlgo(a, gzipConfigs, snappyConfigs, lz4Configs, zstdConfigs, noneConfigs))
            .sum();
        return cfgs * datas.size();
    }

    private static int configCountForAlgo(
        String algo,
        List<Map<String,Integer>> gzipConfigs,
        List<Map<String,Integer>> snappyConfigs,
        List<Map<String,Integer>> lz4Configs,
        List<Map<String,Integer>> zstdConfigs,
        List<Map<String,Integer>> noneConfigs
    ) {
        return switch (algo) {
            case "gzip"   -> gzipConfigs.size();
            case "snappy" -> snappyConfigs.size();
            case "lz4"    -> lz4Configs.size();
            case "zstd"   -> zstdConfigs.size();
            case "none"   -> noneConfigs.size();
            default       -> throw new IllegalArgumentException("Unknown algo: " + algo);
        };
    }

    private static void printProgressBar(int done, int total, long startNs, String label) {
        final int barWidth = 40;
        double frac = total == 0 ? 1.0 : Math.min(1.0, Math.max(0.0, done / (double) total));
        int filled = (int) Math.round(frac * barWidth);

        long now = System.nanoTime();
        long elapsedNs = Math.max(1L, now - startNs);
        double elapsedSec = elapsedNs / 1_000_000_000.0;
        double etaSec = (done == 0) ? Double.NaN : elapsedSec * (total - done) / done;

        String bar = "[" +
            "=".repeat(Math.max(0, filled - 1)) +
            (filled > 0 ? ">" : "") +
            " ".repeat(Math.max(0, barWidth - filled)) +
            "]";

        String pct = String.format(Locale.ROOT, "%3d%%", (int) Math.round(frac * 100));
        String elapsed = formatHMS(elapsedSec);
        String eta = Double.isNaN(etaSec) ? "--:--:--" : formatHMS(etaSec);

        String lbl = label == null ? "" : label;
        if (lbl.length() > 60) lbl = lbl.substring(0, 57) + "...";

        System.out.printf("%s %s %d/%d | elapsed %s | eta %s | %s%n",
            bar, pct, done, total, elapsed, eta, lbl);
    }

    private static String formatHMS(double seconds) {
        int s = (int) Math.round(seconds);
        int h = s / 3600; s %= 3600;
        int m = s / 60;   s %= 60;
        return String.format(Locale.ROOT, "%02d:%02d:%02d", h, m, s);
    }
}