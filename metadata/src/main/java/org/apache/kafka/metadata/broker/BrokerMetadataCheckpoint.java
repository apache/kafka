package org.apache.kafka.metadata.broker;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Optional;
import java.util.Properties;

public class BrokerMetadataCheckpoint {
    private final Logger log;
    private final File file;
    private final Object lock = new Object();

    public BrokerMetadataCheckpoint(File file) {
        this.file = file;
        LogContext logContext = new LogContext("[BrokerMetadataCheckpoint] ");
        this.log = logContext.logger(BrokerMetadataCheckpoint.class);
    }

    public void write(Properties properties) throws IOException {
        synchronized (lock) {
            try {
                File temp = new File(file.getAbsolutePath() + ".tmp");
                FileOutputStream fileOutputStream = new FileOutputStream(temp);
                try {
                    properties.store(fileOutputStream, "");
                    fileOutputStream.flush();
                    fileOutputStream.getFD().sync();
                } finally {
                    Utils.closeQuietly(fileOutputStream, temp.getName());
                }
                Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
            } catch (IOException e) {
                log.error("Failed to write meta.properties due to", e);
                throw e;
            }
        }
    }

    public Optional<Properties> read() throws IOException {
        Files.deleteIfExists(new File(file.getPath() + ".tmp").toPath());

        String absolutePath = file.getAbsolutePath();
        synchronized (lock) {
            try {
                return Optional.of(Utils.loadProps(absolutePath));
            } catch (NoSuchFileException e) {
                log.warn("No meta.properties file under dir " + absolutePath);
                return Optional.empty();
            } catch (Exception e) {
                log.error("Failed to read meta.properties file under dir " + absolutePath, e);
                throw e;
            }
        }
    }
}
