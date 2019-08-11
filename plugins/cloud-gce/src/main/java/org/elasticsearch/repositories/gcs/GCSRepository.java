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

import org.elasticsearch.cloud.gce.GCSService;
import org.elasticsearch.cloud.gce.GCSService.REPOSITORY_GCS;
import org.elasticsearch.cloud.gce.blobstore.GCSBlobStore;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoryName;
import org.elasticsearch.repositories.RepositorySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import com.google.cloud.storage.Storage;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class GCSRepository extends BlobStoreRepository {

	private static final ESLogger logger = Loggers.getLogger(GCSRepository.class);
	
	public static final String TYPE = "gcs";
    public static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB);
	public static final TimeValue NO_TIMEOUT = timeValueMillis(-1);

	private final GCSBlobStore blobStore;

	private final BlobPath basePath;

	private ByteSizeValue chunkSize;

	private boolean compress;

	@Inject
	public GCSRepository(RepositoryName name, RepositorySettings repositorySettings,
			IndexShardRepository indexShardRepository, GCSService gcsService) throws Exception {
		super(name.getName(), repositorySettings, indexShardRepository);

		String bucket = repositorySettings.settings().get("bucket", settings.get(REPOSITORY_GCS.BUCKET));
		String application = repositorySettings.settings().get("application_name",
				settings.get(REPOSITORY_GCS.APPLICATION_NAME, "repository-gcs"));
		String serviceAccount = repositorySettings.settings().get("service_account",
				settings.get(REPOSITORY_GCS.SERVICE_ACCOUNT));

		String basePath = repositorySettings.settings().get("base_path", settings.get(REPOSITORY_GCS.BASE_PATH));
		if (Strings.hasLength(basePath)) {
			BlobPath path = new BlobPath();
			for (String elem : Strings.splitStringToArray(basePath, '/')) {
				path = path.add(elem);
			}
			this.basePath = path;
		} else {
			this.basePath = BlobPath.cleanPath();
		}
		
		this.chunkSize = repositorySettings.settings().getAsBytesSize("chunk_size",
				settings.getAsBytesSize(REPOSITORY_GCS.CHUNK_SIZE, MAX_CHUNK_SIZE));
		this.compress = repositorySettings.settings().getAsBoolean("compress",
				settings.getAsBoolean(REPOSITORY_GCS.COMPRESS, false));

	
		TimeValue connectTimeout = null;
		TimeValue readTimeout = null;

		TimeValue timeout = repositorySettings.settings().getAsTime("read_timeout",
				settings.getAsTime(REPOSITORY_GCS.HTTP_READ_TIMEOUT, NO_TIMEOUT));
		if ((timeout != null) && (timeout.millis() != NO_TIMEOUT.millis())) {
			connectTimeout = timeout;
		}

		timeout = repositorySettings.settings().getAsTime("connect_timeout",
				settings.getAsTime(REPOSITORY_GCS.HTTP_CONNECT_TIMEOUT, NO_TIMEOUT));
		if ((timeout != null) && (timeout.millis() != NO_TIMEOUT.millis())) {
			readTimeout = timeout;
		}
		
		logger.info("Repo settings, bucket: [{}], base path: [{}], application name: [{}], service account: [{}], read timeout: [{}], connect timeout: [{}], compressz; [{}], chunk size: [{}]", 
				bucket,this.basePath,application,serviceAccount,readTimeout, connectTimeout, this.compress, this.chunkSize);

		Storage client = gcsService.createClient(serviceAccount, application, connectTimeout, readTimeout);
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
