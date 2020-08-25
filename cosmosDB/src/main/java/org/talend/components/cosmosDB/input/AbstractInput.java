/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.cosmosDB.input;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.components.cosmosDB.service.CosmosDBService;
import org.talend.components.cosmosDB.service.I18nMessage;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractInput implements Serializable {

    protected InputConfig inputConfig;

    protected final RecordBuilderFactory builderFactory;

    protected CosmosDBService service;

    protected DocumentClient client;

    protected JsonToRecord jsonToRecord;

    protected Iterator<Document> iterator;

    protected I18nMessage i18n;

    AbstractInput(@Option("configuration") final InputConfig configuration, final CosmosDBService service,
            final RecordBuilderFactory builderFactory, final I18nMessage i18n) {
        this.inputConfig = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
        this.i18n = i18n;
    }

    @Producer
    public Record next() {
        if (jsonToRecord == null) {
            jsonToRecord = new JsonToRecord(builderFactory, inputConfig.isJsonForceDouble());
        }
        if (iterator.hasNext()) {
            Document next = iterator.next();
            JsonReader reader = Json.createReader(new StringReader(next.toJson()));
            JsonObject jsonObject = reader.readObject();
            Record record = jsonToRecord.toRecord(jsonObject);
            return record;
        }
        return null;
    }

    @PreDestroy
    public void release() {
        if (client != null) {
            client.close();
        }
    }

    @PostConstruct
    public void init() {
        client = service.documentClientFrom(inputConfig.getDataset().getDatastore());
        iterator = getResults(inputConfig.getDataset().getDatastore().getDatabaseID(),
                inputConfig.getDataset().getCollectionID());
    }

    private Iterator<Document> getResults(String databaseName, String collectionName) {
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        FeedResponse<Document> queryResults;
        if (inputConfig.getDataset().isUseQuery()) {
            // Set some common query options
            FeedOptions queryOptions = new FeedOptions();
            queryOptions.setPageSize(-1);
            queryOptions.setEnableCrossPartitionQuery(true);
            log.debug("query: " + inputConfig.getDataset().getQuery());
            queryResults = this.client.queryDocuments(collectionLink, inputConfig.getDataset().getQuery(), queryOptions);
            log.info("Query [{}] execution success.", inputConfig.getDataset().getQuery());
        } else {
            queryResults = client.readDocuments(collectionLink, null);
        }
        return queryResults.getQueryIterator();
    }
}
