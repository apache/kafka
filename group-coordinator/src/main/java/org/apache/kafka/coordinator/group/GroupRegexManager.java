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

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupRegex.RegexKey;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupRegex.Resolution;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The GroupRegexManager maintains regular expressions used in pattern-based subscription for all consumer groups,
 * resolved using RE2J. It's responsible for:
 * 1) Validating regular expressions.
 * 2) Providing the resolution of a regular expression if available.
 * 3) Evaluating regular expressions to find the matching topics (performed in a separate thread)
 * 4) Re-evaluating regular expressions periodically to keep them up-to-date as topics are created/deleted.
 */
public class GroupRegexManager {

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * Queue containing regular expressions that need to be evaluated.
     */
    private final UniqueBlockingQueue<RegexKey> evalQueue;

    /**
     * The classic and consumer groups keyed by their name.
     */
    private final TimelineHashMap<RegexKey, Resolution> resolvedRegexes;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    /**
     * Indicates if the manager should run the asynchronous eval of requested regular expressions. This is expected
     * to be true from the creation of the manager until there is a call to {@link #onUnloaded()}.
     */
    private boolean runAsyncEval;

    /**
     * Executor to run the thread for evaluating regular expressions.
     */
    private final ExecutorService executorService;

    public static class Builder {

        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry = null;
        private CoordinatorTimer<Void, CoordinatorRecord> timer;

        private MetadataImage metadataImage;

        GroupRegexManager.Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        GroupRegexManager.Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        GroupRegexManager.Builder withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            this.timer = timer;
            return this;
        }

        GroupRegexManager.Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        GroupRegexManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            return new GroupRegexManager(logContext, snapshotRegistry, timer, metadataImage);
        }
    }

    private GroupRegexManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        MetadataImage metadataImage
    ) {
        this.log = logContext.logger(GroupMetadataManager.class);
        this.timer = timer;
        this.metadataImage = metadataImage;
        this.evalQueue = new UniqueBlockingQueue<>(logContext);
        this.resolvedRegexes = new TimelineHashMap<>(snapshotRegistry, 0);
        this.runAsyncEval = true;
        this.executorService = Executors.newSingleThreadExecutor(r -> {
            final Thread thread = new Thread(r,  "coordinatorRegexEvalThread");
            thread.setDaemon(true);
            return thread;
        });
        this.executorService.submit(this::evalRequestedRegexes);
    }

    /**
     * Check if the regex is a valid RE2J regular expression. If it's valid, request an asynchronous eval of the
     * regex against the list of topics from metadata.
     *
     * @param regex The regular expression to validate and evaluate.
     * @throws InvalidRegularExpression If the regex is not a valid RE2J regular expression.
     */
    public Pattern validateAndRequestEval(String groupId, String regex) {
        try {
            Pattern pattern = Pattern.compile(regex);
            maybeRequestEval(groupId, pattern);
            return pattern;
        } catch (PatternSyntaxException e) {
            log.error("Invalid regular expression {}", regex, e);
            throw new InvalidRegularExpression(String.format("SubscribedTopicsPattern %s is not a valid regular expression.", regex));
        }
    }

    /**
     * If the pattern is not resolved yet, request its resolutions by adding it to the evalQueue.
     * Visible for testing.
     */
    void maybeRequestEval(String groupId, Pattern regex) {
        if (!isResolved(groupId, regex)) {
            RegexKey key = new RegexKey.Builder()
                .withGroupId(groupId)
                .withPattern(regex)
                .build();
            evalQueue.offer(key);
        } else {
            log.debug("Regex {} is already resolved for group {}", regex, groupId);
        }
    }


    private void evalRequestedRegexes() {
        while (runAsyncEval) {
            try {
                RegexKey key = evalQueue.take();
                Resolution resolution = maybeEvalRegex(key);
                if (resolution != null) {
                    writeResolvedRegex(key, resolution);
                }
            } catch (Exception e) {
                log.error("Error while evaluating group regex", e);
            }
        }
        log.debug("Stopping regex resolution thread");
    }

    /**
     * Evaluate the given regular expression if it's not resolved yet.
     *
     * @return True if the regular expression is resolved as part of this execution. False if it was already resolved.
     */
    private Resolution maybeEvalRegex(
        RegexKey groupRegexKey
    ) {
        String groupId = groupRegexKey.groupId();
        Pattern pattern = groupRegexKey.pattern();
        if (isResolved(groupId, pattern)) {
            return null;
        }
        long start = System.currentTimeMillis();
        Set<String> allTopics = this.metadataImage.topics().topicsByName().keySet();

        Set<String> matchingTopics = new HashSet<>();
        for (String topic : allTopics) {
            if (pattern.matcher(topic).matches()) {
                matchingTopics.add(topic);
            }
        }
        log.info("Completed evaluating regex {} in {} ms against {} topics. Matching topics found: {}",
            pattern, (System.currentTimeMillis() - start), allTopics.size(), matchingTopics);

        return new Resolution.Builder()
            .withMatchingTopics(matchingTopics)
            .withMetadataVersion(0) // TODO: integrate metadata version
            .build();
    }

    /**
     * Trigger write operation to persist the resolution of the regex.
     *
     * @param key The identifier for the regular expression (groupId and the regular expression).
     * @param resolution The result of the evaluation of the regular expression.
     */
    private void writeResolvedRegex(RegexKey key, Resolution resolution) {
        // TODO: piggybacking on the timer.schedule as initial approach but consider skipping it and trigger write
        //  operation directly.
        timer.schedule(
            regexEvalAttemptKey(key.groupId(), key.pattern().toString(), resolution.metadataVersion()),
            0,
            TimeUnit.MILLISECONDS,
            false,
            () -> consumerGroupRegexResolutionCompleted(key, resolution)
        );
    }

    /**
     * Generate an updated record to persist the regular expression with its matching topics. All regular
     * expressions for a single group are stored in the same record.
     *
     * @return The CoordinatorResult to be applied.
     * @param <T> The type of the CoordinatorResult.
     */
    private <T> CoordinatorResult<T, CoordinatorRecord> consumerGroupRegexResolutionCompleted(
        RegexKey key,
        Resolution resolvedRegex
    ) {
        log.debug("Generating record with newly resolved regex {} for group {}", key.pattern(), key.groupId());
        CoordinatorRecord r = GroupCoordinatorRecordHelpers.newConsumerGroupRegexRecord(key, resolvedRegex);
        return new CoordinatorResult<>(Collections.singletonList(r));
    }

    /**
     * @return String identifying a single resolution for a regular expression (includes the group id, regex and
     * metadata version used). To be used for identifying the operation to persist the results of resolving a regex.
     */
    public static String regexEvalAttemptKey(String groupId, String regex, int metadataVersion) {
        return "regex-eval-" + groupId + "-" + regex + "-" + metadataVersion;
    }

    public void onNewMetadataImage(MetadataImage metadataImage) {
        this.metadataImage = metadataImage;
    }

    /**
     * Stop asynchronous resolution of regular expressions.
     */
    public void onUnloaded() {
        runAsyncEval = false;
        executorService.shutdownNow();
    }

    /**
     * Get regular expressions awaiting to be resolved. Visible for testing.
     *
     * @param group The group ID.
     * @return The set regular expressions awaiting eval for the given group.
     */
    Set<Pattern> awaitingEval(String group) {
        BlockingQueue<RegexKey> awaitingEval = new LinkedBlockingQueue<>(this.evalQueue.queue);
        return awaitingEval.stream()
            .filter(groupRegex -> groupRegex.groupId().equals(group))
            .map(RegexKey::pattern)
            .collect(Collectors.toSet());
    }

    /**
     * @return True if the given regular expression has been already resolved.
     */
    public boolean isResolved(
        String groupId,
        Pattern pattern
    ) {
        RegexKey key = new RegexKey.Builder()
            .withGroupId(groupId)
            .withPattern(pattern).build();
        return this.resolvedRegexes.containsKey(key);
    }


    /**
     * Add the given regex and its resolution to the in-memory store of regular expressions, so it can be used from
     * heartbeats as needed.
     *
     * @param key   Identifier of the regex containing group id and the pattern.
     * @param resolution Resolution of the regex containing the matching topics.
     */
    public void updateRegex(
        RegexKey key,
        Resolution resolution
    ) {
        Resolution oldResolution = this.resolvedRegexes.put(key, resolution);
        log.debug("Completed replaying resolved regex {} for group id {}.",
            key.pattern(), key.groupId());
    }

    /**
     * Remove regex resolution. This will ensure that the regex is not refreshed periodically when not needed anymore.
     *
     * @return True if the regular expression was deleted. False if it didn't exist.
     */
    public boolean removeRegex(
        String groupId,
        String regex
    ) {
        Pattern pattern;
        try {
            pattern = Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            // This is not expected given that only valid regular expressions are evaluated, persisted then
            // deleted (when not in use), so logging and ignore.
            log.error("Ignoring attempt to delete invalid regex {} for group id {}", regex, groupId);
            return false;
        }

        RegexKey key = new RegexKey.Builder()
            .withGroupId(groupId)
            .withPattern(pattern)
            .build();
        return this.resolvedRegexes.remove(key) != null;
    }

    /**
     * Blocking queue without duplicates.
     *
     * @param <T> Type of the elements in the queue.
     */
    private static class UniqueBlockingQueue<T> {
        private final BlockingQueue<T> queue;
        private final Set<T> set;
        private final Logger log;

        public UniqueBlockingQueue(LogContext logContext) {
            this.queue = new LinkedBlockingQueue<>();
            this.set = new HashSet<>();
            this.log = logContext.logger(UniqueBlockingQueue.class);
        }

        public void offer(T s) {
            if (!set.contains(s)) {
                if (!queue.offer(s)) {
                    log.error("Regex queue has reached it's capacity {} and cannot accept new element {}",
                        queue.size(), s);
                }
            }
        }

        public T take() throws InterruptedException {
            T t = queue.take();
            set.remove(t);
            return t;
        }
    }


}
