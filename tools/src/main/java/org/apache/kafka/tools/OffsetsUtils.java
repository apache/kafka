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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.consumer.group.CsvUtils;

import com.fasterxml.jackson.databind.ObjectReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionParser;


public class OffsetsUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(OffsetsUtils.class);
    private static final String TOPIC_PARTITION_SEPARATOR = ":";
    private final Admin adminClient;
    private final OffsetsUtilsOptions opts;
    private final OptionParser parser;

    public OffsetsUtils(Admin adminClient, OptionParser parser, OffsetsUtilsOptions opts) {
        this.adminClient = adminClient;
        this.opts = opts;
        this.parser = parser;
    }

    public static void printOffsetsToReset(Map<String, Map<TopicPartition, OffsetAndMetadata>> groupAssignmentsToReset) {
        int maxGroupLen = Math.max(15, groupAssignmentsToReset.keySet().stream().mapToInt(String::length).max().orElse(0));
        int maxTopicLen = Math.max(15, groupAssignmentsToReset.values().stream()
            .flatMap(assignments -> assignments.keySet().stream())
            .mapToInt(tp -> tp.topic().length())
            .max()
            .orElse(0));

        String format = "%n%" + (-maxGroupLen) + "s %" + (-maxTopicLen) + "s %-10s %s";
        if (!groupAssignmentsToReset.isEmpty())
            System.out.printf(format, "GROUP", "TOPIC", "PARTITION", "NEW-OFFSET");

        groupAssignmentsToReset.forEach((groupId, assignment) ->
            assignment.forEach((consumerAssignment, offsetAndMetadata) ->
                System.out.printf(format,
                    groupId,
                    consumerAssignment.topic(),
                    consumerAssignment.partition(),
                    offsetAndMetadata.offset())));
        System.out.println();
    }

    public Optional<Map<String, Map<TopicPartition, OffsetAndMetadata>>> resetPlanFromFile() {
        if (opts.resetFromFileOpt != null && !opts.resetFromFileOpt.isEmpty()) {
            try {
                String resetPlanPath = opts.resetFromFileOpt.get(0);
                String resetPlanCsv = Utils.readFileAsString(resetPlanPath);
                Map<String, Map<TopicPartition, OffsetAndMetadata>> resetPlan = parseResetPlan(resetPlanCsv);
                return Optional.of(resetPlan);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else return Optional.empty();
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> parseResetPlan(String resetPlanCsv) {
        ObjectReader csvReader = CsvUtils.readerFor(CsvUtils.CsvRecordNoGroup.class);
        String[] lines = resetPlanCsv.split("\n");
        boolean isSingleGroupQuery = opts.groupOpt.size() == 1;
        boolean isOldCsvFormat = false;
        try {
            if (lines.length > 0) {
                csvReader.readValue(lines[0], CsvUtils.CsvRecordNoGroup.class);
                isOldCsvFormat = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Ignore.
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> dataMap = new HashMap<>();

        try {
            // Single group CSV format: "topic,partition,offset"
            if (isSingleGroupQuery && isOldCsvFormat) {
                String group = opts.groupOpt.get(0);
                for (String line : lines) {
                    CsvUtils.CsvRecordNoGroup rec = csvReader.readValue(line, CsvUtils.CsvRecordNoGroup.class);
                    dataMap.computeIfAbsent(group, k -> new HashMap<>())
                        .put(new TopicPartition(rec.getTopic(), rec.getPartition()), new OffsetAndMetadata(rec.getOffset()));
                }
            } else {
                csvReader = CsvUtils.readerFor(CsvUtils.CsvRecordWithGroup.class);
                for (String line : lines) {
                    CsvUtils.CsvRecordWithGroup rec = csvReader.readValue(line, CsvUtils.CsvRecordWithGroup.class);
                    dataMap.computeIfAbsent(rec.getGroup(), k -> new HashMap<>())
                        .put(new TopicPartition(rec.getTopic(), rec.getPartition()), new OffsetAndMetadata(rec.getOffset()));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return dataMap;
    }

    private Map<TopicPartition, Long> checkOffsetsRange(Map<TopicPartition, Long> requestedOffsets) {
        Map<TopicPartition, LogOffsetResult> logStartOffsets = getLogStartOffsets(requestedOffsets.keySet());
        Map<TopicPartition, LogOffsetResult> logEndOffsets = getLogEndOffsets(requestedOffsets.keySet());

        Map<TopicPartition, Long> res = new HashMap<>();

        requestedOffsets.forEach((topicPartition, offset) -> {
            LogOffsetResult logEndOffset = logEndOffsets.get(topicPartition);

            if (logEndOffset != null) {
                if (logEndOffset instanceof LogOffset && offset > ((LogOffset) logEndOffset).value) {
                    long endOffset = ((LogOffset) logEndOffset).value;
                    LOGGER.warn("New offset (" + offset + ") is higher than latest offset for topic partition " + topicPartition + ". Value will be set to " + endOffset);
                    res.put(topicPartition, endOffset);
                } else {
                    LogOffsetResult logStartOffset = logStartOffsets.get(topicPartition);

                    if (logStartOffset instanceof LogOffset && offset < ((LogOffset) logStartOffset).value) {
                        long startOffset = ((LogOffset) logStartOffset).value;
                        LOGGER.warn("New offset (" + offset + ") is lower than earliest offset for topic partition " + topicPartition + ". Value will be set to " + startOffset);
                        res.put(topicPartition, startOffset);
                    } else
                        res.put(topicPartition, offset);
                }
            } else {
                // the control should not reach here
                throw new IllegalStateException("Unexpected non-existing offset value for topic partition " + topicPartition);
            }
        });

        return res;
    }

    private Map<TopicPartition, LogOffsetResult> getLogTimestampOffsets(Collection<TopicPartition> topicPartitions, long timestamp) {
        try {
            Map<TopicPartition, OffsetSpec> timestampOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.forTimestamp(timestamp)));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = adminClient.listOffsets(
                timestampOffsets,
                withTimeoutMs(new ListOffsetsOptions())
            ).all().get();

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> successfulOffsetsForTimes = new HashMap<>();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> unsuccessfulOffsetsForTimes = new HashMap<>();

            offsets.forEach((tp, offsetsResultInfo) -> {
                if (offsetsResultInfo.offset() != ListOffsetsResponse.UNKNOWN_OFFSET)
                    successfulOffsetsForTimes.put(tp, offsetsResultInfo);
                else
                    unsuccessfulOffsetsForTimes.put(tp, offsetsResultInfo);
            });

            Map<TopicPartition, LogOffsetResult> successfulLogTimestampOffsets = successfulOffsetsForTimes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new LogOffset(e.getValue().offset())));

            unsuccessfulOffsetsForTimes.forEach((tp, offsetResultInfo) ->
                System.out.println("\nWarn: Partition " + tp.partition() + " from topic " + tp.topic() +
                    " is empty. Falling back to latest known offset."));

            successfulLogTimestampOffsets.putAll(getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet()));

            return successfulLogTimestampOffsets;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<TopicPartition, LogOffsetResult> getLogStartOffsets(Collection<TopicPartition> topicPartitions) {
        return getLogOffsets(topicPartitions, OffsetSpec.earliest());
    }

    public Map<TopicPartition, LogOffsetResult> getLogEndOffsets(Collection<TopicPartition> topicPartitions) {
        return getLogOffsets(topicPartitions, OffsetSpec.latest());
    }

    public Map<TopicPartition, LogOffsetResult> getLogOffsets(Collection<TopicPartition> topicPartitions, OffsetSpec offsetSpec) {
        try {
            Map<TopicPartition, OffsetSpec> startOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), tp -> offsetSpec));

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = adminClient.listOffsets(
                startOffsets,
                withTimeoutMs(new ListOffsetsOptions())
            ).all().get();

            return topicPartitions.stream().collect(Collectors.toMap(
                Function.identity(),
                tp -> offsets.containsKey(tp)
                    ? new LogOffset(offsets.get(tp).offset())
                    : new Unknown()
            ));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TopicPartition> parseTopicPartitionsToReset(List<String> topicArgs) throws ExecutionException, InterruptedException {
        List<String> topicsWithPartitions = new ArrayList<>();
        List<String> topics = new ArrayList<>();

        topicArgs.forEach(topicArg -> {
            if (topicArg.contains(TOPIC_PARTITION_SEPARATOR))
                topicsWithPartitions.add(topicArg);
            else
                topics.add(topicArg);
        });

        List<TopicPartition> specifiedPartitions =
            topicsWithPartitions.stream().flatMap(this::parseTopicsWithPartitions).collect(Collectors.toList());

        List<TopicPartition> unspecifiedPartitions = new ArrayList<>();

        if (!topics.isEmpty()) {
            Map<String, TopicDescription> descriptionMap = adminClient.describeTopics(
                topics,
                withTimeoutMs(new DescribeTopicsOptions())
            ).allTopicNames().get();

            descriptionMap.forEach((topic, description) ->
                description.partitions().forEach(tpInfo -> unspecifiedPartitions.add(new TopicPartition(topic, tpInfo.partition())))
            );
        }

        specifiedPartitions.addAll(unspecifiedPartitions);

        return specifiedPartitions;
    }

    public Stream<TopicPartition> parseTopicsWithPartitions(String topicArg) {
        ToIntFunction<String> partitionNum = partition -> {
            try {
                return Integer.parseInt(partition);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid partition '" + partition + "' specified in topic arg '" + topicArg + "''");
            }
        };

        String[] arr = topicArg.split(":");

        if (arr.length != 2)
            throw new IllegalArgumentException("Invalid topic arg '" + topicArg + "', expected topic name and partitions");

        String topic = arr[0];
        String partitions = arr[1];

        return Arrays.stream(partitions.split(",")).
            map(partition -> new TopicPartition(topic, partitionNum.applyAsInt(partition)));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetToOffset(Collection<TopicPartition> partitionsToReset) {
        long offset = opts.resetToOffsetOpt != null && !opts.resetToOffsetOpt.isEmpty()
            ? opts.resetToOffsetOpt.get(0)
            : 0L;
        return checkOffsetsRange(partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), tp -> offset)))
            .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetToEarliest(Collection<TopicPartition> partitionsToReset) {
        Map<TopicPartition, LogOffsetResult> logStartOffsets = getLogStartOffsets(partitionsToReset);
        return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
            LogOffsetResult logOffsetResult = logStartOffsets.get(topicPartition);

            if (!(logOffsetResult instanceof LogOffset)) {
                CommandLineUtils.printUsageAndExit(parser, "Error getting starting offset of topic partition: " + topicPartition);
            }

            return new OffsetAndMetadata(((LogOffset) logOffsetResult).value);
        }));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetToLatest(Collection<TopicPartition> partitionsToReset) {
        Map<TopicPartition, LogOffsetResult> logEndOffsets = getLogEndOffsets(partitionsToReset);
        return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
            LogOffsetResult logOffsetResult = logEndOffsets.get(topicPartition);

            if (!(logOffsetResult instanceof LogOffset)) {
                CommandLineUtils.printUsageAndExit(parser, "Error getting ending offset of topic partition: " + topicPartition);
            }

            return new OffsetAndMetadata(((LogOffset) logOffsetResult).value);
        }));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetByShiftBy(
        Collection<TopicPartition> partitionsToReset,
        Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets) {

        Map<TopicPartition, Long> requestedOffsets = partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
            long shiftBy = opts.resetShiftByOpt;
            OffsetAndMetadata currentOffset = currentCommittedOffsets.get(topicPartition);

            if (currentOffset == null) {
                throw new IllegalArgumentException("Cannot shift offset for partition " + topicPartition + " since there is no current committed offset");
            }

            return currentOffset.offset() + shiftBy;
        }));
        return checkOffsetsRange(requestedOffsets).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetToDateTime(Collection<TopicPartition> partitionsToReset) {
        try {
            long timestamp = Utils.getDateTime(opts.resetToDatetimeOpt.get(0));
            Map<TopicPartition, LogOffsetResult> logTimestampOffsets =
                getLogTimestampOffsets(partitionsToReset, timestamp);
            return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                LogOffsetResult logTimestampOffset = logTimestampOffsets.get(topicPartition);
                if (!(logTimestampOffset instanceof LogOffset)) {
                    CommandLineUtils.printUsageAndExit(parser, "Error getting offset by timestamp of topic partition: " + topicPartition);
                }
                return new OffsetAndMetadata(((LogOffset) logTimestampOffset).value);
            }));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> resetByDuration(Collection<TopicPartition> partitionsToReset) {
        String duration = opts.resetByDurationOpt;
        Duration durationParsed = Duration.parse(duration);
        Instant now = Instant.now();
        durationParsed.negated().addTo(now);
        long timestamp = now.minus(durationParsed).toEpochMilli();
        Map<TopicPartition, OffsetsUtils.LogOffsetResult> logTimestampOffsets =
            getLogTimestampOffsets(partitionsToReset, timestamp);
        return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
            OffsetsUtils.LogOffsetResult logTimestampOffset = logTimestampOffsets.get(topicPartition);

            if (!(logTimestampOffset instanceof OffsetsUtils.LogOffset)) {
                CommandLineUtils.printUsageAndExit(parser, "Error getting offset by timestamp of topic partition: " + topicPartition);
            }

            return new OffsetAndMetadata(((OffsetsUtils.LogOffset) logTimestampOffset).value);
        }));
    }

    public Map<TopicPartition, OffsetAndMetadata> resetFromFile(String groupId) {
        return resetPlanFromFile().map(resetPlan -> {
            Map<TopicPartition, OffsetAndMetadata> resetPlanForGroup = resetPlan.get(groupId);

            if (resetPlanForGroup == null) {
                printError("No reset plan for group " + groupId + " found", Optional.empty());
                return Map.<TopicPartition, OffsetAndMetadata>of();
            }

            Map<TopicPartition, Long> requestedOffsets = resetPlanForGroup.keySet().stream().collect(Collectors.toMap(
                Function.identity(),
                topicPartition -> resetPlanForGroup.get(topicPartition).offset()));

            return checkOffsetsRange(requestedOffsets).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
        }).orElseGet(Map::of);
    }

    public Map<TopicPartition, OffsetAndMetadata> resetToCurrent(Collection<TopicPartition> partitionsToReset, Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets) {
        Collection<TopicPartition> partitionsToResetWithCommittedOffset = new ArrayList<>();
        Collection<TopicPartition> partitionsToResetWithoutCommittedOffset = new ArrayList<>();

        for (TopicPartition topicPartition : partitionsToReset) {
            if (currentCommittedOffsets.containsKey(topicPartition))
                partitionsToResetWithCommittedOffset.add(topicPartition);
            else
                partitionsToResetWithoutCommittedOffset.add(topicPartition);
        }

        Map<TopicPartition, OffsetAndMetadata> preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.stream()
            .collect(Collectors.toMap(Function.identity(), topicPartition -> {
                OffsetAndMetadata committedOffset = currentCommittedOffsets.get(topicPartition);

                if (committedOffset == null) {
                    throw new IllegalStateException("Expected a valid current offset for topic partition: " + topicPartition);
                }

                return new OffsetAndMetadata(committedOffset.offset());
            }));

        Map<TopicPartition, OffsetAndMetadata> preparedOffsetsForPartitionsWithoutCommittedOffset =
            getLogEndOffsets(partitionsToResetWithoutCommittedOffset)
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    if (!(e.getValue() instanceof OffsetsUtils.LogOffset)) {
                        CommandLineUtils.printUsageAndExit(parser, "Error getting ending offset of topic partition: " + e.getKey());
                    }
                    return new OffsetAndMetadata(((OffsetsUtils.LogOffset) e.getValue()).value);
                }));

        preparedOffsetsForPartitionsWithCommittedOffset.putAll(preparedOffsetsForPartitionsWithoutCommittedOffset);

        return preparedOffsetsForPartitionsWithCommittedOffset;
    }

    public void checkAllTopicPartitionsValid(Collection<TopicPartition> partitionsToReset) {
        // check the partitions exist
        List<TopicPartition> partitionsNotExistList = filterNonExistentPartitions(partitionsToReset);
        if (!partitionsNotExistList.isEmpty()) {
            String partitionStr = partitionsNotExistList.stream().map(TopicPartition::toString).collect(Collectors.joining(","));
            throw new UnknownTopicOrPartitionException("The partitions \"" + partitionStr + "\" do not exist");
        }

        // check the partitions have leader
        List<TopicPartition> partitionsWithoutLeader = filterNoneLeaderPartitions(partitionsToReset);
        if (!partitionsWithoutLeader.isEmpty()) {
            String partitionStr = partitionsWithoutLeader.stream().map(TopicPartition::toString).collect(Collectors.joining(","));
            throw new LeaderNotAvailableException("The partitions \"" + partitionStr + "\" have no leader");
        }
    }

    public List<TopicPartition> filterNoneLeaderPartitions(Collection<TopicPartition> topicPartitions) {
        // collect all topics
        Set<String> topics = topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());

        try {
            return adminClient.describeTopics(topics, withTimeoutMs(new DescribeTopicsOptions())).allTopicNames().get().entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().partitions().stream()
                    .filter(partitionInfo -> partitionInfo.leader() == null)
                    .map(partitionInfo -> new TopicPartition(entry.getKey(), partitionInfo.partition())))
                    .filter(topicPartitions::contains)
                .toList();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<TopicPartition> filterNonExistentPartitions(Collection<TopicPartition> topicPartitions) {
        // collect all topics
        Set<String> topics = topicPartitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
        try {
            List<TopicPartition> existPartitions = adminClient.describeTopics(topics, withTimeoutMs(new DescribeTopicsOptions())).allTopicNames().get().entrySet()
                .stream()
                .flatMap(entry -> entry.getValue().partitions().stream()
                    .map(partitionInfo -> new TopicPartition(entry.getKey(), partitionInfo.partition())))
                .toList();

            return topicPartitions.stream().filter(tp -> !existPartitions.contains(tp)).toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
        int t = (int) opts.timeoutMsOpt;
        return options.timeoutMs(t);
    }

    private static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    public interface LogOffsetResult { }

    public record LogOffset(long value) implements LogOffsetResult {
    }

    public static class Unknown implements LogOffsetResult { }

    public static class Ignore implements LogOffsetResult { }


    public static class OffsetsUtilsOptions {
        List<String> groupOpt;
        List<Long> resetToOffsetOpt;
        List<String> resetFromFileOpt;
        List<String> resetToDatetimeOpt;
        String resetByDurationOpt;
        Long resetShiftByOpt;
        long timeoutMsOpt;

        public OffsetsUtilsOptions(
            List<String> groupOpt,
            List<Long> resetToOffsetOpt,
            List<String> resetFromFileOpt,
            List<String> resetToDatetimeOpt,
            String resetByDurationOpt,
            Long resetShiftByOpt,
            long timeoutMsOpt) {

            this.groupOpt = groupOpt;
            this.resetToOffsetOpt = resetToOffsetOpt;
            this.resetFromFileOpt = resetFromFileOpt;
            this.resetToDatetimeOpt = resetToDatetimeOpt;
            this.resetByDurationOpt = resetByDurationOpt;
            this.resetShiftByOpt = resetShiftByOpt;
            this.timeoutMsOpt = timeoutMsOpt;
        }

        public OffsetsUtilsOptions(
            List<String> groupOpt,
            List<String> resetToDatetimeOpt,
            long timeoutMsOpt) {

            this.groupOpt = groupOpt;
            this.resetToDatetimeOpt = resetToDatetimeOpt;
            this.timeoutMsOpt = timeoutMsOpt;
        }
    }
}
