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

package org.elasticsearch.cloud.gcs;

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

	private Environment env;

	@Inject
	public GCSServiceImpl(Environment env) {
		super(env.settings());
		this.env = env;
	}

	@Override
	public Storage createClient(final String serviceAccount, final String application, final TimeValue connectTimeout,
			final TimeValue readTimeout) throws IOException, GeneralSecurityException {
		
		final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
		builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
		final HttpTransport httpTransport = builder.build();
		
		final HttpTransportOptions httpTransportOptions = HttpTransportOptions.newBuilder()
				.setConnectTimeout(RepoUtil.toTimeout(connectTimeout))
				.setReadTimeout(RepoUtil.toTimeout(readTimeout))				
				.setHttpTransportFactory(new HttpTransportFactory() {
					@Override
					public HttpTransport create() {
						return httpTransport;
					}
				})
				.build();
		
		final ServiceAccountCredentials serviceAccountCredentials = loadCredentials(serviceAccount);

		
		final String projectId = getProjectId(serviceAccountCredentials);
		
		logger.debug("Project ID: [{}]", projectId);
		logger.debug("initializing client with service account [{}]", serviceAccountCredentials.toString());
				
		return StorageOptions.newBuilder()			
				.setCredentials(serviceAccountCredentials)
				.setTransportOptions(httpTransportOptions)	
				.setProjectId(projectId)
				.build()
				.getService();
	}

	/**
	 * Retrieve the cloud project Id from the service account
	 * @param serviceAccountCredentials
	 * @return Project Id
	 */
	private String getProjectId(ServiceAccountCredentials serviceAccountCredentials) {
		
		//Assuming that the client email is from the same cloud project
		
		String[] emailParts = serviceAccountCredentials.getClientEmail().split("@");		
		String[] domainPart = emailParts[1].split("\\.");			
		return domainPart[0];		
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
		if (!Files.exists(account)) {
			throw new ElasticsearchException(
					"Unable to find service account file [" + serviceAccount + "] defined for repository");
		}
		else {
			logger.info("Found service account: [{}]", account.toAbsolutePath());
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
	protected void doStart() {
		logger.debug("Started Component GCSServiceImpl");
	}

	@Override
	protected void doStop() {
		logger.debug("Stopped Component GCSServiceImpl");
	}

	@Override
	protected void doClose() {
		logger.debug("Closed Component GCSServiceImpl");
	}	
}
