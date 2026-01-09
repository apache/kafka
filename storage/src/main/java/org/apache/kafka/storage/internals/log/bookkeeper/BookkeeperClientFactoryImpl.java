package org.apache.kafka.storage.internals.log.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class BookkeeperClientFactoryImpl implements BookkeeperClientFactory  {
    @Override
    public CompletableFuture<BookKeeper> create(LogConfig conf, MetadataStoreExtended store,
                                                EventLoopGroup eventLoopGroup,
                                                Map<String, Object> properties) {
        return create(conf, store, eventLoopGroup, properties, NullStatsLogger.INSTANCE);
    }

    @Override
    public CompletableFuture<BookKeeper> create(LogConfig conf, MetadataStoreExtended store,
                                                EventLoopGroup eventLoopGroup,
                                                Map<String, Object> properties, StatsLogger statsLogger) {
        PulsarMetadataClientDriver.init();

        ClientConfiguration bkConf = createBkClientConfiguration(store, conf);
        if (properties != null) {
            properties.forEach(bkConf::setProperty);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                return getBookKeeperBuilder(conf, statsLogger, bkConf, eventLoopGroup).build();
            } catch (InterruptedException | BKException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @VisibleForTesting
    BookKeeper.Builder getBookKeeperBuilder(LogConfig conf, StatsLogger statsLogger, ClientConfiguration bkConf, EventLoopGroup eventLoopGroup) {
        BookKeeper.Builder builder = BookKeeper.forConfig(bkConf)
                .allocator(PulsarByteBufAllocator.DEFAULT)
                .statsLogger(statsLogger);
        if (eventLoopGroup != null) {
            builder.eventLoopGroup(eventLoopGroup);
        }
        return builder;
    }

    @VisibleForTesting
    ClientConfiguration createBkClientConfiguration(MetadataStoreExtended store, LogConfig conf) {
        ClientConfiguration bkConf = new ClientConfiguration();
        if (conf.bookkeeperClientAuthenticationPlugin != null
                && !conf.bookkeeperClientAuthenticationPlugin.trim().isEmpty()) {
            bkConf.setClientAuthProviderFactoryClass(conf.bookkeeperClientAuthenticationPlugin);
            bkConf.setProperty(conf.bookkeeperClientAuthenticationParametersName,
                    conf.bookkeeperClientAuthenticationParameters);
        }

        if (conf.bookkeeperTLSClientAuthentication) {
            bkConf.setTLSClientAuthentication(true);
            bkConf.setTLSCertificatePath(conf.bookkeeperTLSCertificateFilePath);
            bkConf.setTLSKeyStore(conf.bookkeeperTLSKeyFilePath);
            bkConf.setTLSKeyStoreType(conf.bookkeeperTLSKeyFileType);
            bkConf.setTLSKeyStorePasswordPath(conf.bookkeeperTLSKeyStorePasswordPath);
            bkConf.setTLSProviderFactoryClass(conf.bookkeeperTLSProviderFactoryClass);
            bkConf.setTLSTrustStore(conf.bookkeeperTLSTrustCertsFilePath);
            bkConf.setTLSTrustStoreType(conf.bookkeeperTLSTrustCertTypes);
            bkConf.setTLSTrustStorePasswordPath(conf.bookkeeperTLSTrustStorePasswordPath);
            bkConf.setTLSCertFilesRefreshDurationSeconds(conf.bookkeeperTlsCertFilesRefreshDurationSeconds);
        }

        bkConf.setBusyWaitEnabled(conf.enableBusyWait);
        bkConf.setNumWorkerThreads(conf.bookkeeperClientNumWorkerThreads);
        bkConf.setThrottleValue(conf.bookkeeperClientThrottleValue);
        bkConf.setZkTimeout(conf.bookkeeperMetadataSessionTimeoutMillis);
        bkConf.setAddEntryTimeout(conf.bookkeeperClientTimeoutInSeconds);
        bkConf.setReadEntryTimeout(conf.bookkeeperClientTimeoutInSeconds);
        bkConf.setSpeculativeReadTimeout(conf.bookkeeperClientSpeculativeReadTimeoutInMillis);
        bkConf.setNumChannelsPerBookie(conf.bookkeeperNumberOfChannelsPerBookie);
        bkConf.setUseV2WireProtocol(conf.bookkeeperUseV2WireProtocol);
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setStickyReadsEnabled(conf.bookkeeperEnableStickyReads);
        bkConf.setNettyMaxFrameSizeBytes(conf.bookkeeperNettyMaxFrameSizeBytes);
        bkConf.setDiskWeightBasedPlacementEnabled(conf.bookkeeperDiskWeightBasedPlacementEnabled);
        bkConf.setMetadataServiceUri(conf.bookkeeperMetadataServiceUrl);
        bkConf.setLimitStatsLogging(conf.bookkeeperClientLimitStatsLogging);

        if (StringUtils.isEmpty(conf.bookkeeperMetadataServiceUrl)) {
            // If we're connecting to the same metadata service, with same config, then
            // let's share the MetadataStore instance
            bkConf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        }

        if (conf.bookkeeperClientHealthCheckEnabled) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval(conf.bookkeeperClientHealthCheckIntervalSeconds, TimeUnit.SECONDS);
            bkConf.setBookieErrorThresholdPerInterval(conf.bookkeeperClientHealthCheckErrorThresholdPerInterval);
            bkConf.setBookieQuarantineTime(conf.bookkeeperClientHealthCheckQuarantineTimeInSeconds, TimeUnit.SECONDS);
            bkConf.setBookieQuarantineRatio(conf.bookkeeperClientQuarantineRatio);
        }

        bkConf.setReorderReadSequenceEnabled(conf.bookkeeperClientReorderReadSequenceEnabled);
        bkConf.setExplictLacInterval(conf.bookkeeperExplicitLacIntervalInMills);
        bkConf.setGetBookieInfoIntervalSeconds(conf.bookkeeperClientGetBookieInfoIntervalSeconds, TimeUnit.SECONDS);
        bkConf.setGetBookieInfoRetryIntervalSeconds(conf.bookkeeperClientGetBookieInfoRetryIntervalSeconds, TimeUnit.SECONDS);
        bkConf.setNumIOThreads(conf.bookkeeperClientNumIoThreads);
        return bkConf;
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
