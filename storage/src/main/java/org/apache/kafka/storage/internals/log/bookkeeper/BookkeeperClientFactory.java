package org.apache.kafka.storage.internals.log.bookkeeper;

import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface BookkeeperClientFactory {
    CompletableFuture<BookKeeper> create(LogConfig conf, MetadataStoreExtended store,
                                         EventLoopGroup eventLoopGroup,
                                         Map<String, Object> properties);

    CompletableFuture<BookKeeper> create(LogConfig conf, MetadataStoreExtended store,
                                         EventLoopGroup eventLoopGroup,
                                         Map<String, Object> properties, StatsLogger statsLogger);

    void close();
}
