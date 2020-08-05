/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.gcs.blobstore;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.gcs.RepoUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.NoSuchFileException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class GCSBlobStore extends AbstractComponent implements BlobStore {

    // The recommended maximum size of a blob that should be uploaded in a single
    // request. Larger files should be uploaded over multiple requests (this is
    // called "resumable upload")
    // https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
    private static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE = 5 * 1024 * 1024;

    private final Storage storage;
    private final String bucket;

    public GCSBlobStore(Settings settings, String bucket, Storage storage) {
        super(settings);
        this.bucket = bucket;
        this.storage = storage;

        if (!doesBucketExist(bucket)) {
            throw new BlobStoreException("Bucket [" + bucket + "] does not exist");
        }
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        logger.info("Set blob container with path: {}", path);
        return new GCSBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) throws IOException {
        logger.debug("Delete path: {}", path.toString());
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        logger.debug("Delete path: {}", keyPath);
        deleteBlobsByPrefix(keyPath);
    }

    @Override
    public void close() {
        logger.debug("Closed GCSBlobStore");
    }

    /**
     * Return true if the given bucket exists
     *
     * @param bucketName name of the bucket
     * @return true if the bucket exists, false otherwise
     */
    boolean doesBucketExist(final String bucketName) {
        logger.debug("Checking if bucekt exists: {}", bucketName);
        try {
            return RepoUtil.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws Exception {
                    return storage.get(bucketName) != null;
                }
            });
        } catch (IOException e) {
            logger.error("Error checking if bucket exists: {}", e, bucketName);
            throw new BlobStoreException("Unable to check if bucket [" + bucketName + "] exists", e);
        }
    }

    /**
     * List all blobs in the bucket
     *
     * @param path base path of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobs(final String path) throws IOException {
        return listBlobsByPrefix(path, "");
    }

    /**
     * List all blobs in the bucket which have a prefix
     *
     * @param path   base path of the blobs to list
     * @param prefix prefix of the blobs to list
     * @return a map of blob names and their metadata
     */
    Map<String, BlobMetaData> listBlobsByPrefix(final String path, final String prefix) throws IOException {
        logger.debug("List blobs for path: {}, prefix: {}", path, prefix);
        final String pathPrefix = buildKey(path, prefix);
        logger.debug("Path prefix: {}", pathPrefix);
        final MapBuilder<String, BlobMetaData> mapBuilder = MapBuilder.newMapBuilder();

        final AtomicLong counter = new AtomicLong();

        try {
            return RepoUtil.doPrivileged(new PrivilegedExceptionAction<Map<String, BlobMetaData>>() {
                @Override
                public Map<String, BlobMetaData> run() throws Exception {
                    Page<Blob> page = storage.get(bucket).list(BlobListOption.prefix(pathPrefix));

                    String nextPageToken = page.getNextPageToken();
                    Iterator<Blob> currentPageIterator = page.getValues().iterator();
                    counter.addAndGet(processIterator(currentPageIterator, path, mapBuilder));

                    while (nextPageToken != null) {
                        page = storage.get(bucket)
                            .list(Storage.BlobListOption.prefix(pathPrefix),
                                Storage.BlobListOption.pageToken(nextPageToken));
                        currentPageIterator = page.getValues().iterator();
                        nextPageToken = page.getNextPageToken();
                        counter.addAndGet(processIterator(currentPageIterator, path, mapBuilder));
                    }

                    return mapBuilder.immutableMap();
                }
            });
        } catch (Exception e) {
            logger.info("Failed after {}", counter.get());
            logger.error("Error on prefix {}: {}", e, pathPrefix, e);
            throw e;
        }
    }

    private long processIterator(Iterator<Blob> iterator, String path, MapBuilder<String, BlobMetaData> mapBuilder) {
        long counter = 0;
        while (iterator.hasNext()) {
            Blob blob = iterator.next();
            logger.debug("Blob: {}", blob.toString());
            assert blob.getName().startsWith(path);
            final String suffixName = blob.getName().substring(path.length());
            logger.debug("Suffix: {}", suffixName);
            mapBuilder.put(suffixName, new PlainBlobMetaData(suffixName, blob.getSize()));
            counter++;
        }
        return counter;
    }

    private Blob getBlob(final BlobId blobId) throws IOException {
        return RepoUtil.doPrivileged(new PrivilegedExceptionAction<Blob>() {
            @Override
            public Blob run() throws Exception {
                return storage.get(blobId);
            }
        });
    }

    /**
     * Returns true if the blob exists in the bucket
     *
     * @param blobName name of the blob
     * @return true if the blob exists, false otherwise
     */
    boolean blobExists(String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        logger.debug("Blob id: [{}]", blobId);

        Blob blob = getBlob(blobId);

        return blob != null;
    }

    /**
     * Returns an {@link java.io.InputStream} for a given blob
     *
     * @param blobName name of the blob
     * @return an InputStream
     */
    InputStream readBlob(final String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        logger.debug("Blob id: [{}]", blobId);
        final Blob blob = getBlob(blobId);

        if (blob == null) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exit");
        }

        final ReadChannel readChannel = RepoUtil.doPrivileged(new PrivilegedExceptionAction<ReadChannel>() {
            @Override
            public ReadChannel run() throws Exception {
                return blob.reader();
            }
        });

        return Channels.newInputStream(new ReadableByteChannel() {
            @Override
            public boolean isOpen() {
                return readChannel.isOpen();
            }

            @Override
            public void close() throws IOException {
                RepoUtil.doPrivilegedVoid(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        readChannel.close();
                        return null;
                    }
                });
            }

            @Override
            public int read(final ByteBuffer dst) throws IOException {
                return RepoUtil.doPrivileged(new PrivilegedExceptionAction<Integer>() {
                    @Override
                    public Integer run() throws Exception {
                        return readChannel.read(dst);
                    }
                });
            }
        });
    }

    /**
     * Writes a blob in the bucket.
     *
     * @param blobName    name of the blob
     * @param inputStream content of the blob to be written
     * @param blobSize    expected size of the blob to be written
     */
    void writeBlob(final String blobName, final InputStream inputStream, final long blobSize) throws IOException {

        final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, blobName).build();

        logger.debug("Blob info: {}, size: {}, size threshold: {}", blobInfo, blobSize, LARGE_BLOB_THRESHOLD_BYTE_SIZE);

        if (blobSize > LARGE_BLOB_THRESHOLD_BYTE_SIZE) {
            writeBlobResumable(blobInfo, inputStream);
        } else {
            writeBlobMultipart(blobInfo, inputStream, blobSize);
        }
    }

    /**
     * Uploads a blob using the "resumable upload" method (multiple requests, which
     * can be independently retried in case of failure, see
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
     *
     * @param blobInfo    the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     */
    private void writeBlobResumable(final BlobInfo blobInfo, InputStream inputStream) throws IOException {
        logger.info("Running resumable blob write");
        final WriteChannel writeChannel = RepoUtil.doPrivileged(new PrivilegedExceptionAction<WriteChannel>() {
            @Override
            public WriteChannel run() throws Exception {
                return storage.writer(blobInfo);
            }
        });

        Streams.copy(inputStream, Channels.newOutputStream(new WritableByteChannel() {
            @Override
            public boolean isOpen() {
                return writeChannel.isOpen();
            }

            @Override
            public void close() throws IOException {
                RepoUtil.doPrivilegedVoid(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        writeChannel.close();
                        return null;
                    }
                });
            }

            @SuppressForbidden(reason = "Channel is based of a socket not a file")
            @Override
            public int write(final ByteBuffer src) throws IOException {
                return RepoUtil.doPrivileged(new PrivilegedExceptionAction<Integer>() {
                    @Override
                    public Integer run() throws Exception {
                        return writeChannel.write(src);
                    }
                });
            }
        }));
    }

    /**
     * Uploads a blob using the "multipart upload" method (a single
     * 'multipart/related' request containing both data and metadata. The request is
     * gziped), see:
     * https://cloud.google.com/storage/docs/json_api/v1/how-tos/multipart-upload
     *
     * @param blobInfo    the info for the blob to be uploaded
     * @param inputStream the stream containing the blob data
     * @param blobSize    the size
     */
    private void writeBlobMultipart(final BlobInfo blobInfo, InputStream inputStream, long blobSize)
        throws IOException {
        logger.info("Running multipart blob write");
        assert blobSize <= LARGE_BLOB_THRESHOLD_BYTE_SIZE : "large blob uploads should use the resumable upload method";

        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) blobSize);
        Streams.copy(inputStream, baos);

        Blob blob = RepoUtil.doPrivileged(new PrivilegedExceptionAction<Blob>() {
            @Override
            public Blob run() throws Exception {
                try {
                    return storage.create(blobInfo, baos.toByteArray());
                } catch (final StorageException se) {
                    logger.error("Failed to upload data to bucket: {}, Error: {}", se, blobInfo.getBlobId().getName(), se.getMessage());

                    throw new IOException(se.getCause());
                }
            }
        });

        logger.info("Created Blob: [{}]", blob);
    }

    /**
     * @param blobName name of the blob
     * @param bytes
     * @throws IOException
     */
    public void writeBlob(final String blobName, final BytesReference bytes) throws IOException {
        StreamInput input = StreamInput.wrap(bytes);
        writeBlob(blobName, input, bytes.length());
    }

    /**
     * Deletes a blob in the bucket
     *
     * @param blobName name of the blob
     */
    void deleteBlob(final String blobName) throws IOException {
        final BlobId blobId = BlobId.of(bucket, blobName);
        logger.debug("Blob id: {}", blobId);

        final boolean deleted = RepoUtil.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                return storage.delete(blobId);
            }
        });

        if (!deleted) {
            throw new NoSuchFileException("Blob [" + blobName + "] does not exist");
        }
    }

    /**
     * Deletes multiple blobs in the bucket that have a given prefix
     *
     * @param prefix prefix of the buckets to delete
     */
    void deleteBlobsByPrefix(final String prefix) throws IOException {
        deleteBlobs(listBlobsByPrefix("", prefix).keySet());
    }

    /**
     * Deletes multiple blobs in the given bucket (uses a batch request to perform
     * this)
     *
     * @param blobNames names of the bucket to delete
     */
    void deleteBlobs(final Collection<String> blobNames) throws IOException {
        if (blobNames == null || blobNames.isEmpty()) {
            return;
        }

        if (blobNames.size() == 1) {
            deleteBlob(blobNames.iterator().next());
            return;
        }

        final List<BlobId> blobIdsToDelete = new ArrayList<>();
        for (String blobName : blobNames) {
            blobIdsToDelete.add(BlobId.of(bucket, blobName));
        }

        final List<Boolean> deletedStatuses = RepoUtil.doPrivileged(new PrivilegedExceptionAction<List<Boolean>>() {
            @Override
            public List<Boolean> run() throws Exception {
                return storage.delete(blobIdsToDelete);
            }
        });

        assert blobIdsToDelete.size() == deletedStatuses.size();
        boolean failed = false;
        for (int i = 0; i < blobIdsToDelete.size(); i++) {
            if (Boolean.FALSE.equals(deletedStatuses.get(i))) {
                logger.error("Failed to delete blob [{}] in bucket [{}]", blobIdsToDelete.get(i).getName(), bucket);
                failed = true;
            }
        }
        if (failed) {
            throw new IOException("Failed to delete all [" + blobIdsToDelete.size() + "] blobs");
        }
    }

    /**
     * Moves a blob within the same bucket
     *
     * @param sourceBlob name of the blob to move
     * @param targetBlob new name of the blob in the target bucket
     */
    void moveBlob(final String sourceBlobName, final String targetBlobName) throws IOException {
        logger.info("Moving Blobs from: {}, to: {}", sourceBlobName, targetBlobName);
        final BlobId sourceBlobId = BlobId.of(bucket, sourceBlobName);
        final BlobId targetBlobId = BlobId.of(bucket, targetBlobName);

        final CopyRequest request = CopyRequest.newBuilder().setSource(sourceBlobId).setTarget(targetBlobId).build();

        RepoUtil.doPrivilegedVoid(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // There's no atomic "move" in GCS so we need to copy and delete
                Blob copiedBlob = storage.copy(request).getResult();
                logger.info("Copied Blob: [{}]", copiedBlob.toString());
                final boolean deleted = storage.delete(sourceBlobId);
                if (!deleted) {
                    throw new IOException(
                        "Failed to move source [" + sourceBlobName + "] to target [" + targetBlobName + "]");
                } else {
                    logger.info("Deleted source blob: {}", sourceBlobId);
                }

                return null;
            }
        });
    }

    private String buildKey(String keyPath, String s) {
        assert s != null;
        return keyPath + s;
    }
}
