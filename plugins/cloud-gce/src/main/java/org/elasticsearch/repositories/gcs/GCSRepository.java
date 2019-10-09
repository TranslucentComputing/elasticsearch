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

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cloud.gcs.GCSService;
import org.elasticsearch.cloud.gcs.GCSService.RepositoryGCS;
import org.elasticsearch.cloud.gcs.blobstore.GCSBlobStore;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import com.google.cloud.storage.Storage;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class GCSRepository extends BlobStoreRepository {
	
	public static final String TYPE = "gcs";
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB);
	public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);

	private final GCSBlobStore blobStore;

	private final BlobPath basePath;

	private ByteSizeValue chunkSize;

	private boolean compress;

	@Inject
	public GCSRepository(RepositoryName name, RepositorySettings repositorySettings,
			IndexShardRepository indexShardRepository, GCSService gcsService) throws IOException, GeneralSecurityException {
		super(name.getName(), repositorySettings, indexShardRepository);

		String bucket = repositorySettings.settings().get("bucket", settings.get(RepositoryGCS.BUCKET));
		String application = repositorySettings.settings().get("application_name",
				settings.get(RepositoryGCS.APPLICATION_NAME, "repository-gcs"));
		String serviceAccount = repositorySettings.settings().get("service_account",
				settings.get(RepositoryGCS.SERVICE_ACCOUNT));

		String configPath = repositorySettings.settings().get("base_path", settings.get(RepositoryGCS.BASE_PATH));
		if (Strings.hasLength(configPath)) {
			BlobPath path = new BlobPath();
			for (String elem : Strings.splitStringToArray(configPath, '/')) {
				path = path.add(elem);
			}
			this.basePath = path;
		} else {
			this.basePath = BlobPath.cleanPath();
		}
		
		this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size",
				settings.getAsBytesSize(RepositoryGCS.CHUNK_SIZE, MAX_CHUNK_SIZE));
		this.compress = repositorySettings.settings().getAsBoolean("compress",
				settings.getAsBoolean(RepositoryGCS.COMPRESS, false));

	
		TimeValue connectTimeout = null;
		TimeValue readTimeout = null;

		TimeValue timeout = repositorySettings.settings().getAsTime("read_timeout",
				settings.getAsTime(RepositoryGCS.HTTP_READ_TIMEOUT, NO_TIMEOUT));
		if ((timeout != null) && (timeout.millis() != NO_TIMEOUT.millis())) {
			connectTimeout = timeout;
		}

		timeout = repositorySettings.settings().getAsTime("connect_timeout",
				settings.getAsTime(RepositoryGCS.HTTP_CONNECT_TIMEOUT, NO_TIMEOUT));
		if ((timeout != null) && (timeout.millis() != NO_TIMEOUT.millis())) {
			readTimeout = timeout;
		}
		
		logger.info("Repo settings, bucket: [{}], base path: [{}], application name: [{}], service account: [{}], read timeout: [{}], connect timeout: [{}], compressz; [{}], chunk size: [{}]", 
				bucket,this.basePath,application,serviceAccount,readTimeout, connectTimeout, this.compress, this.chunkSize);

		Storage client = gcsService.createClient(serviceAccount, application, connectTimeout, readTimeout);
		
		logger.info("Created storage client");
		
		this.blobStore = new GCSBlobStore(settings, bucket, client);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected BlobStore blobStore() {
		return blobStore;
	}

	@Override
	protected BlobPath basePath() {
		return basePath;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean isCompress() {
		return compress;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected ByteSizeValue chunkSize() {
		return chunkSize;
	}

}
