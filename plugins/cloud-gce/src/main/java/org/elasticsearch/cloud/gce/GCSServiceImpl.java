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

package org.elasticsearch.cloud.gce;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.services.storage.StorageScopes;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.gcs.RepoUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;

public class GCSServiceImpl extends AbstractLifecycleComponent<GCSService> implements GCSService {

	private static final ESLogger logger = Loggers.getLogger(GCSServiceImpl.class);

	private Environment env;

	@Inject
	public GCSServiceImpl(Environment env) {
		super(env.settings());
		this.env = env;
	}

	@Override
	public Storage createClient(final String serviceAccount, final String application, final TimeValue connectTimeout,
			final TimeValue readTimeout) throws Exception {

		final HttpTransportOptions httpTransportOptions = HttpTransportOptions.newBuilder()
				.setConnectTimeout(RepoUtil.toTimeout(connectTimeout))
				.setReadTimeout(RepoUtil.toTimeout(readTimeout))				
				.setHttpTransportFactory(new HttpTransportFactory() {

					@Override
					public HttpTransport create() {
						final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
						try {
							// requires java.lang.RuntimePermission "setFactory"
							builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
						} catch (GeneralSecurityException | IOException e) {
							logger.error("Failed to set trusted cert for google http transport", e);							
						}
						return builder.build();
					}
				}).build();

		final ServiceAccountCredentials serviceAccountCredentials = loadCredentials(serviceAccount);

		logger.debug("initializing client with service account [{}]", serviceAccountCredentials.toString());
				
		return StorageOptions.newBuilder()			
				.setCredentials(serviceAccountCredentials)
				.setTransportOptions(httpTransportOptions)		
				.build()
				.getService();
	}

	/**
	 * HTTP request initializer that loads credentials from the service account file
	 * and manages authentication for HTTP requests
	 */
	private ServiceAccountCredentials loadCredentials(String serviceAccount) throws IOException {
		if (serviceAccount == null) {
			throw new ElasticsearchException("Cannot load Google Cloud Storage service account file from a null path");
		}

		Path account = env.configFile().resolve(serviceAccount);
		if (Files.exists(account) == false) {
			throw new ElasticsearchException(
					"Unable to find service account file [" + serviceAccount + "] defined for repository");
		}

		try (InputStream is = Files.newInputStream(account)) {
			return RepoUtil.doPrivileged(new PrivilegedExceptionAction<ServiceAccountCredentials>() {
				@Override
				public ServiceAccountCredentials run() throws Exception {
					final Collection<String> scopes = Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL);
					final ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(is);
					if (credentials.createScopedRequired()) {
						return (ServiceAccountCredentials) credentials.createScoped(scopes);
					}
					return credentials;
				}
			});
		}
	}

	@Override
	protected void doStart() throws ElasticsearchException {
		logger.debug("Started Component GCSServiceImpl");
	}

	@Override
	protected void doStop() throws ElasticsearchException {
		logger.debug("Stopped Component GCSServiceImpl");
	}

	@Override
	protected void doClose() throws ElasticsearchException {
		logger.debug("Closed Component GCSServiceImpl");
	}	
}
