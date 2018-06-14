package org.apache.kafka.streams.state.internals;

import org.rocksdb.BackupEngine;
import org.rocksdb.BackupInfo;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Checkpoint the local state to remote storage. We use checkpointFuture to record
 * the current checkpoint job in-progress on the separate thread. If the job takes too long (over the checkpoint
 * interval), we shall shut down the thread and disable further upload.
 */
public class RemoteCheckpoint {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteCheckpoint.class);

    private final RocksDB db;
    private final String dbName;
    private final String taskId;
    private final String backupDirName;
    private final String s3Bucket;
    private final String s3PathPrefix;
    private ExecutorService executorService;
    private final long intervalMs;
    private Future<Void> checkpointFuture;
    private long lastRemoteCheckpointStartTimeMs = 0L;

    public RemoteCheckpoint(RocksDB db,
                            final String taskId,
                            String dbName,
                            String backupDirName,
                            String s3Bucket,
                            String s3PathPrefix,
                            long intervalMs) throws Exception {
        this.db = db;
        this.taskId = taskId;
        this.dbName = dbName;
        this.backupDirName = backupDirName;
        this.s3Bucket = s3Bucket;
        this.s3PathPrefix = s3PathPrefix;
        setupExecutor();
        this.intervalMs = intervalMs;
        // run the checkpoint upon restart, so that we could verify it works right away.
        runOnce();
    }

    private void setupExecutor() {
        this.executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r, taskId + "-remote-checkpoint");
                thread.setDaemon(true);
                return thread;
            }
        });
        LOG.info("Remote checkpoint thread started.");
    }

    private void shutdownExecutor() {
        try {
            executorService.shutdown();
            executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
            LOG.info("Remote checkpoint thread closed.");
        } catch (InterruptedException e) {
            // ignore interruption.
        }
    }

    private File createDbCopy() throws Exception {
        File backupDir = new File(backupDirName);
        if (!backupDir.exists() && !backupDir.mkdirs()) {
            throw new Exception("Cannot create backup directory " + backupDirName);
        }

        String localBackupPath = backupDirName + "/" + dbName;
        try {
            File localBackupFile = new File(localBackupPath);
            if (localBackupFile.exists() && !localBackupFile.delete()) {
                throw new IOException("Cannot delete the backup db file: " + localBackupPath);
            }
            BackupableDBOptions backupOptions = new BackupableDBOptions(localBackupPath);
            // Disable the WAL
            backupOptions.setBackupLogFiles(false);
            // no need to sync since we use the backup only as intermediate data before writing to
            // FileSystem snapshot
            backupOptions.setSync(true);
            // backupOptions.setSync(false);
            BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions);

            List<BackupInfo> backupInfos = backupEngine.getBackupInfo();
            if (!backupInfos.isEmpty()) {
                throw new Exception("back up info is empty");
            }
            long start = System.currentTimeMillis();
            backupEngine.createNewBackup(db, true);
            long end = System.currentTimeMillis();
            long timeElapsed = end - start;
            LOG.info("Local backup takes {}", timeElapsed);
        } catch (Exception e) {
            LOG.error("Exception with RocksDB backup at {}: {}", localBackupPath, e);
        }
        return new File(localBackupPath);
    }

    private void uploadDbToS3(final File dbBackup) {
        checkpointFuture = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // Update checkpoint start time, so that we could trigger a second checkpoint when timeout.
                    lastRemoteCheckpointStartTimeMs = System.currentTimeMillis();
                    String s3Path = s3PathPrefix + "/" + dbBackup.getName();
                    LOG.info("Uploading to {}", s3Path);
                    AwsS3Client.uploadDirectory(dbBackup, s3Bucket, s3Path);
                    long end = System.currentTimeMillis();
                    long timeElapsed = end - lastRemoteCheckpointStartTimeMs;
                    LOG.info("Upload to remote storage takes {}", timeElapsed);
                } catch (Exception e) {
                    LOG.error("Exception uploading {} to S3", dbBackup.getAbsolutePath(), e);
                } finally {
                    if (!dbBackup.delete()) {
                        LOG.error("failed to delete last db store backup");
                    }
                }
            }
        }, null);
        LOG.info("Uploading to S3 job submitted.");
    }

    public void runOnce() throws Exception {
        // No current checkpoint running
        if (checkpointFuture == null || checkpointFuture.isDone()) {
            // Create a backup copy of current db
            final File dbBackup = createDbCopy();
            uploadDbToS3(dbBackup);
            return;
        }

        LOG.info("Last checkpoint takes too long to finish, ready to cancel it");
        // If we have a hanged thread, we just close it for now.
        if (!checkpointFuture.cancel(true)) {
            LOG.error("Cancellation of the current uploading failed, closing the thread and restart...");
            shutdownExecutor();
            setupExecutor();
        }
    }

    /**
     * External service will call this periodically to see whether to checkpoint.
     * @return true if we should trigger another checkpoint.
     */
    public boolean shouldCheckpoint() {
        return (System.currentTimeMillis() - lastRemoteCheckpointStartTimeMs) > intervalMs;
    }

    public void close() {
        shutdownExecutor();
    }
}
