package org.apache.kafka.streams.state.internals;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

public class AwsS3Client {
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3Client.class);
    private static volatile AwsS3Client mInstance;
    private static String FOLDER_MARK = "_$folder$";
    private static Integer THREAD_POOL_SIZE = 256;

    /** When done with the Parquet output, Spark creates a _SUCCESS file in the output directory. */
    private static String SUCCESS_SUFFIX = "/_SUCCESS";

    private final TransferManager transferManager;
    private final AmazonS3Client s3Client;

    protected AwsS3Client() {
        ClientConfiguration conf = new ClientConfiguration()
                .withMaxConnections(THREAD_POOL_SIZE);
        s3Client = new AmazonS3Client(conf);
        transferManager = new TransferManager(s3Client, Executors.newFixedThreadPool(THREAD_POOL_SIZE));
    }

    public static AwsS3Client getInstance() {
        if (mInstance == null) {
            synchronized (AwsS3Client.class) {
                if (mInstance == null) {
                    mInstance = new AwsS3Client();
                }
            }
        }
        return mInstance;
    }

    public static void setInstance(AwsS3Client a) {
        synchronized (AwsS3Client.class) {
            mInstance = a;
        }
    }

    public static String[] list(String bucket, String key) {
        return list(bucket, key, false);
    }

    public static String[] list(String bucket, String key, Boolean recursive) {
        return getInstance().listImpl(bucket, key, recursive);
    }

    public static String listLast(String bucket, String key) {
        String[] files = getInstance().listImpl(bucket, key, false);
        if (files == null || files.length == 0) {
            return null;
        }
        return files[files.length - 1];
    }

    public static String listKthLast(String bucket, String key, int k) {
        String[] files = getInstance().listImpl(bucket, key, false);
        if (files == null || files.length < k) {
            return null;
        }
        return files[files.length - k];
    }

    /**
     * Lists the directories in S3 at given bucket and key prefix that contain _SUCCESS files.
     * Returns the list of directories, sorted in a lexicographical order, which is useful when all
     * the directories only differ in the last segment of their path, and it has the format of e.g.
     * epoch=YYYY-MM-DD-HH-mm. In that case, the returned directories are sorted by their dates.
     */
    public static String[] listSuccessDirs(String bucket, String key) {
        String[] files = list(bucket, key, true);
        List<String> dirs = new ArrayList<>();
        for (String file : files) {
            if (file.endsWith(SUCCESS_SUFFIX)) {
                dirs.add(file.substring(0, file.length() - SUCCESS_SUFFIX.length()));
            }
        }
        Collections.sort(dirs);
        return dirs.toArray(new String[]{});
    }

    /**
     * Returns the last directory in S3 at given bucket and key prefix that contains _SUCCESS files.
     * TODO: instead of listing all the files under the key prefix recursively, consider listing just
     * the immediate sub-directories first, and check them for _SUCCESS files one at a time.
     */
    public static String listLastSuccessDir(String bucket, String key) {
        String[] dirs = listSuccessDirs(bucket, key);
        if (dirs == null || dirs.length == 0) {
            return null;
        }
        return dirs[dirs.length - 1];
    }

    public static void upload(File file, String bucket, String path)
        throws AmazonClientException, InterruptedException {
        getInstance().uploadImpl(file, bucket, path);
    }

    public static void uploadSlow(File file, String bucket, String path, ProgressListener listener)
            throws AmazonClientException, InterruptedException {
        AmazonS3 s3Client = new AmazonS3Client();
        TransferManager tm = new TransferManager(s3Client);
        Upload upload = tm.upload(bucket, path, file);
        if (listener != null) {
            upload.addProgressListener(listener);
        }
        upload.waitForCompletion();
        tm.shutdownNow(true);
    }

    public static void uploadDirectory(File directory, String bucket, String path)
            throws AmazonClientException, InterruptedException {
        getInstance().uploadDirectoryImpl(directory, bucket, path);
    }

    public static File download(String bucket, String key, File localPath)
            throws AmazonClientException, InterruptedException {
        if (localPath.isDirectory()) {
            localPath = new File(localPath, new File(key).getName());
        }
        return getInstance().downloadImpl(bucket, key, localPath);
    }

    public static File downloadDirectory(String bucket, String keyPrefix, File localDir)
            throws AmazonClientException, InterruptedException {
        return getInstance().downloadDirectoryImpl(bucket, keyPrefix, localDir);
    }

    public static void delete(String bucket, String key)
            throws AmazonClientException {
        getInstance().deleteImpl(bucket, key);
    }

    public static InputStream read(String bucket, String key)
            throws IOException, InterruptedException {
        return getInstance().readImpl(bucket, key);
    }

    public static void shutdown(boolean shutDownS3Client) {
        getInstance().shutdownImpl(shutDownS3Client);
    }

    public String[] listImpl(String bucket, String key, Boolean recursive) {
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(key);

        if (!recursive) {
            request = request.withDelimiter("/");
        }

        ObjectListing listing = s3Client.listObjects(request);

        List<String> ret = extractListing(listing);

        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            ret.addAll(extractListing(listing));
        }

        Collections.sort(ret);
        return ret.toArray(new String[]{});
    }

    public void uploadImpl(File file, String bucket, String path)
            throws AmazonClientException, InterruptedException {
        Upload upload = transferManager.upload(bucket, path, file);
        upload.waitForCompletion();
    }

    public void uploadDirectoryImpl(File directory, String bucket, String path)
            throws AmazonClientException, InterruptedException {
        MultipleFileUpload upload = transferManager.uploadDirectory(bucket, path, directory, true);
        upload.waitForCompletion();
    }

    public File downloadImpl(String bucket, String key, File localPath)
            throws AmazonClientException, InterruptedException {
        Download download = transferManager.download(bucket, key, localPath);
        download.waitForCompletion();
        return localPath;
    }

    public File downloadDirectoryImpl(String bucket, String keyPrefix, File localDir)
            throws AmazonClientException, InterruptedException {
        MultipleFileDownload
            download = transferManager.downloadDirectory(bucket, keyPrefix, localDir);
        download.waitForCompletion();
        return localDir;
    }

    public void deleteImpl(String bucket, String key) throws AmazonClientException {
        getInstance().s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
    }

    public InputStream readImpl(String bucket, String key)
            throws IOException, InterruptedException {
        int idx = key.lastIndexOf("/");
        String prefix;
        if (idx == -1) {
            prefix = key;
        } else {
            prefix = key.substring(idx + 1);
        }
        File dir = Files.createTempDirectory(prefix).toFile();
        File file = download(bucket, key, dir);
        InputStream is = new FileInputStream(file);
        // The file should be deleted up on closing
        // We may think about whether we want to have this later
        // FileUtils.deleteQuietly(dir);
        return is;
    }

    private List<String> extractListing(ObjectListing listing) {
        List<String> ret = new ArrayList<>();

        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        for (S3ObjectSummary s : summaries) {
            String path = s.getKey();
            if (path.endsWith(FOLDER_MARK)) {
                continue;
            }
            ret.add(path);
        }
        ret.addAll(listing.getCommonPrefixes());
        return ret;
    }

    private void shutdownImpl(boolean shutDownS3Client) {
        transferManager.shutdownNow(shutDownS3Client);
    }
}
