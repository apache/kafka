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
package kafka.log.remote;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import scala.Option;
import scala.jdk.javaapi.OptionConverters;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

public class RemoteLogOffsetReader implements Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogOffsetReader.class);
    private final RemoteLogManager rlm;
    private final TopicPartition tp;
    private final long timestamp;
    private final long startingOffset;
    private final LeaderEpochFileCache leaderEpochCache;
    private final Supplier<Option<FileRecords.TimestampAndOffset>> searchInLocalLog;
    private final Consumer<Either<Exception, Option<FileRecords.TimestampAndOffset>>> callback;

    public RemoteLogOffsetReader(RemoteLogManager rlm,
                                 TopicPartition tp,
                                 long timestamp,
                                 long startingOffset,
                                 LeaderEpochFileCache leaderEpochCache,
                                 Supplier<Option<FileRecords.TimestampAndOffset>> searchInLocalLog,
                                 Consumer<Either<Exception, Option<FileRecords.TimestampAndOffset>>> callback) {
        this.rlm = rlm;
        this.tp = tp;
        this.timestamp = timestamp;
        this.startingOffset = startingOffset;
        this.leaderEpochCache = leaderEpochCache;
        this.searchInLocalLog = searchInLocalLog;
        this.callback = callback;
    }

    @Override
    public Void call() throws Exception {
        Either<Exception, Option<FileRecords.TimestampAndOffset>> result;
        try {
            // If it is not found in remote storage, then search in the local storage starting with local log start offset.
            Option<FileRecords.TimestampAndOffset> timestampAndOffsetOpt =
                    OptionConverters.toScala(rlm.findOffsetByTimestamp(tp, timestamp, startingOffset, leaderEpochCache))
                    .orElse(searchInLocalLog::get);
            result = Right.apply(timestampAndOffsetOpt);
        } catch (Exception e) {
            // NOTE: All the exceptions from the secondary storage are catched instead of only the KafkaException.
            LOGGER.error("Error occurred while reading the remote log offset for {}", tp, e);
            result = Left.apply(e);
        }
        callback.accept(result);
        return null;
    }
}
