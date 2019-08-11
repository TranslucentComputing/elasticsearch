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

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.unit.TimeValue;

import com.google.cloud.storage.Storage;

public interface GCSService extends LifecycleComponent<GCSService> {

	final class REPOSITORY_GCS {
		public static final String BUCKET = "repositories.gcs.bucket";
		public static final String BASE_PATH = "repositories.gcs.base_path";
		public static final String APPLICATION_NAME = "repositories.gcs.application_name";
		public static final String SERVICE_ACCOUNT = "repositories.gcs.service_account";
		public static final String HTTP_READ_TIMEOUT = "repositories.gcs.http.read_timeout";
		public static final String HTTP_CONNECT_TIMEOUT = "repositories.gcs.http.connect_timeout";
        public static final String CHUNK_SIZE = "repositories.gcs.chunk_size";
        public static final String COMPRESS = "repositories.gcs.compress";        
	}
	
	Storage createClient(String serviceAccount,String application,TimeValue connectTimeout, TimeValue readTimeout) throws Exception;
}
