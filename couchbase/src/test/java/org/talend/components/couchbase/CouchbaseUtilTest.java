/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.couchbase;

import java.util.Arrays;
import java.util.List;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public abstract class CouchbaseUtilTest {

    protected static final String BUCKET_NAME = "student";

    protected static final String BUCKET_PASSWORD = "secret";

    private static final int BUCKET_QUOTA = 100;

    private static final String CLUSTER_USERNAME = "student";

    private static final String CLUSTER_PASSWORD = "secret";

    private static final int DEFAULT_TIMEOUT_IN_SEC = 40;

    private static final List<String> ports = Arrays.asList("8091:8091", "8092:8092", "8093:8093", "8094:8094", "11210:11210");

    private static final CouchbaseContainer COUCHBASE_CONTAINER;

    protected static final CouchbaseCluster COUCHBASE_CLUSTER;

    protected final CouchbaseDataStore couchbaseDataStore;

    @Injected
    protected BaseComponentsHandler componentsHandler;

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    static {
        COUCHBASE_CONTAINER = new CouchbaseContainer("couchbase/server:6.5.1").withCredentials(CLUSTER_USERNAME, CLUSTER_PASSWORD)
                .withBucket(new BucketDefinition(BUCKET_NAME).withQuota(BUCKET_QUOTA));
        COUCHBASE_CONTAINER.setPortBindings(ports);
        COUCHBASE_CONTAINER.start();
        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC)
                .bootstrapCarrierDirectPort(COUCHBASE_CONTAINER.getBootstrapCarrierDirectPort())
                .bootstrapHttpDirectPort(COUCHBASE_CONTAINER.getBootstrapHttpDirectPort()).build();

        COUCHBASE_CLUSTER = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getHost());
    }

    public CouchbaseUtilTest() {
        couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);
    }

    protected String generateDocId(String prefix, int number) {
        return prefix + "_" + number;
    }
}
