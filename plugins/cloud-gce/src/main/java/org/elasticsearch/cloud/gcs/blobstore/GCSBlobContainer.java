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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class GCSBlobContainer extends AbstractBlobContainer {

	private final ESLogger logger = Loggers.getLogger(getClass());		
	private final GCSBlobStore blobStore;
	private final String path;

	protected GCSBlobContainer(BlobPath path, GCSBlobStore blobStore) {
		super(path);
		this.blobStore = blobStore;
		String keyPath = path.buildAsString("/");
		if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        this.path = keyPath;
        
        logger.debug("Path: {}", this.path);
	}

	@Override
	public boolean blobExists(String blobName) {
		logger.debug("Check if blob {} exists", blobName);
		try {
			return blobStore.blobExists(buildKey(blobName));
		} catch (Exception e) {		
			throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
		}
	}

	@Override
	public Map<String, BlobMetaData> listBlobs() throws IOException {
		logger.debug("List all blobs");
		return blobStore.listBlobs(path);
	}

	@Override
	public Map<String, BlobMetaData> listBlobsByPrefix(String prefix) throws IOException {
		logger.debug("List all blobs by prefix: {}", prefix);
		return blobStore.listBlobsByPrefix(path, prefix);
	}

	@Override
	public InputStream readBlob(String blobName) throws IOException {
		logger.debug("Read blob {}", blobName);
		return blobStore.readBlob(buildKey(blobName));
	}

	@Override
	public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
		logger.debug("InputStream - Write to blob {}", blobName);
				
		if (blobExists(blobName)) {
			throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
		}
		blobStore.writeBlob(buildKey(blobName), inputStream, blobSize);
	}
	
	@Override
	public void writeBlob(String blobName, BytesReference bytes) throws IOException {	
		logger.debug("BytesReference - Write to blob {}", blobName);

		if (blobExists(blobName)) {
			throw new FileAlreadyExistsException("blob [" + blobName + "] already exists, cannot overwrite");
		}
		blobStore.writeBlob(buildKey(blobName), bytes);
	}

	@Override
	public void deleteBlob(String blobName) throws IOException {
		logger.debug("Delete to blob {}", blobName);
		blobStore.deleteBlob(buildKey(blobName));
	}

	@Override
	public void move(String sourceBlobName, String targetBlobName) throws IOException {
		logger.debug("Move blob from {} to {}", sourceBlobName,targetBlobName);
		blobStore.moveBlob(buildKey(sourceBlobName), buildKey(targetBlobName));
	}

	protected String buildKey(String blobName) {
		assert blobName != null;
		return path + blobName;
	}
}
