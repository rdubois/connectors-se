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
package org.talend.components.bigquery.output;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.talend.sdk.component.api.component.Icon.IconType.BIGQUERY;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@Processor(name = "BigQueryOutput")
@Documentation("This component writes into BigQuery.")
public class BigQueryOutput implements Serializable {

    /** Maximum records per request allowed by Google API */
    private static final int MAX_BATCH_SIZE = 10_000;

    private final I18nMessage i18n;

    private final BigQueryOutputConfig configuration;

    private final BigQueryConnection connection;

    private Schema tableSchema;

    private transient List<Record> records;

    private transient boolean init;

    private transient BigQuery bigQuery;

    private transient TableId tableId;

    private BigQueryService service;

    private GoogleStorageService storageService;

    private transient Storage storage;

    private final RecordIORepository ioRepository;

    private RecordWriter recordWriter;

    private String blobName;

    private BlobInfo blobInfo;

    public BigQueryOutput(@Option("configuration") final BigQueryOutputConfig configuration, BigQueryService bigQueryService, final GoogleStorageService storageService, RecordIORepository ioRepository,
            I18nMessage i18n) {
        this.configuration = configuration;
        this.connection = configuration.getDataSet().getConnection();
        this.tableSchema = bigQueryService.guessSchema(configuration);
        this.service = bigQueryService;
        this.storageService = storageService;
        this.ioRepository = ioRepository;
        this.i18n = i18n;
        truncateTableIfNeeded();
    }

    private void truncateTableIfNeeded() {
        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {
            bigQuery = service.createClient(connection);
            tableId = TableId.of(connection.getProjectName(), configuration.getDataSet().getBqDataset(),
                    configuration.getDataSet().getTableName());
            Table table = bigQuery.getTable(tableId);
            if (table == null) {
                throw new BigQueryConnectorException(i18n.infoTableNoExists(
                        configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
            }
            QueryJobConfiguration queryConfig = QueryJobConfiguration
                    .newBuilder("TRUNCATE TABLE  `" + connection.getProjectName() + "."
                            + configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName() + "`")
                    .setUseLegacySql(false).build();
            long time = System.currentTimeMillis();
            Job job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(getNewUniqueJobId(bigQuery)).build());
            long deleteTime = System.currentTimeMillis() - time;
            log.info("BigQuery sql truncate request took " + deleteTime + " ms");
            try {
                job = job.waitFor();
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
            }
            if (job.isDone()) {
                log.info(i18n.infoQueryDone());
            }
        }
    }

    private JobId getNewUniqueJobId(BigQuery bigQuery) {
        JobId jobId;
        do {
            jobId = JobId.of(UUID.randomUUID().toString());
        } while (bigQuery.getJob(jobId) != null);
        return jobId;
    }

    @PostConstruct
    public void init() {
        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {
            bigQuery = service.createClient(connection);
            storage = storageService.getStorage(bigQuery.getOptions().getCredentials());
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        records = new ArrayList<>();
        Blob blob = getNewBlob();
        WriteChannel writer = blob.writer();
        try {
            recordWriter = buildWriter(writer);
        } catch (IOException e) {
            log.warn(e.getMessage());
        }
    }

    private Blob getNewBlob() {
        Blob blob;
        do {
            String uuid = UUID.randomUUID().toString();
            blobName = "temp/" + uuid + "/" + tableId.getTable() + ".avro";
            blobInfo = BlobInfo.newBuilder(configuration.getDataSet().getGsBucket(), blobName).build();
            blob = storage.get(blobInfo.getBlobId());
            if (blob == null) {
                blob = storage.create(blobInfo);
            }
        } while (blob == null);
        return blob;
    }

    private RecordWriter buildWriter(WriteChannel writerChannel) throws IOException {
        final ContentFormat contentFormat = new AvroConfiguration();
        final RecordWriterSupplier recordWriterSupplier = this.ioRepository.findWriter(contentFormat.getClass());

        final RecordWriter writer = recordWriterSupplier.getWriter(() -> Channels.newOutputStream(writerChannel), contentFormat);
        writer.init(contentFormat);
        return writer;
    }

    @ElementListener
    public void onElement(Record record) {
        if (!init) {
            lazyInit();
        }
        records.add(record);
    }

    private void lazyInit() {
        init = true;
        bigQuery = service.createClient(connection);
        tableId = TableId.of(connection.getProjectName(), configuration.getDataSet().getBqDataset(),
                configuration.getDataSet().getTableName());
        Table table = bigQuery.getTable(tableId);
        if (table != null) {
            tableSchema = table.getDefinition().getSchema();
        } else if (configuration.getTableOperation() != BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            throw new BigQueryConnectorException(i18n.infoTableNoExists(
                    configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
        }
    }

    @AfterGroup
    public void afterGroup() {

        if (!records.isEmpty() && tableSchema == null
                && configuration.getTableOperation() == BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            tableSchema = service.guessSchema(records.get(0));

            if (tableSchema != null) {
                try {
                    Table table = bigQuery.getTable(tableId);
                    if (table == null) {
                        log.info(i18n.infoTableNoExists(
                                configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
                        TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
                        table = bigQuery.create(tableInfo);
                        log.info(i18n.infoTableCreated(tableId.getTable()));
                    }
                } catch (BigQueryException e) {
                    log.warn(i18n.errorCreationTable() + e.getMessage(), e);

                }
            }
            if (tableSchema == null) {
                // Retry reading table metadata, in case another worker created if meanwhile
                Table table = bigQuery.getTable(tableId);
                if (table != null) {
                    tableSchema = table.getDefinition().getSchema();
                }
            }
        }

        TacoKitRecordToTableRowConverter converter = new TacoKitRecordToTableRowConverter(tableSchema, i18n);

        int nbRecordsToSend = records.size();
        int nbRecordsSent = 0;
        List<Map<String, ?>> recordsBuffer = new ArrayList<>();

        while (nbRecordsSent < nbRecordsToSend) {

            recordsBuffer.clear();
            records.stream().skip(nbRecordsSent).limit(MAX_BATCH_SIZE).map(converter::apply).forEach(recordsBuffer::add);

            InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            recordsBuffer.stream().forEach(insertAllRequestBuilder::addRow);
            InsertAllResponse response = bigQuery.insertAll(insertAllRequestBuilder.build());

            if (response.hasErrors()) {
                // response.getInsertErrors();
                // rejected no handled by TCK
                log.warn(i18n.warnRejected(response.getInsertErrors().size()));
                // log errors for first row
                response.getInsertErrors().values().iterator().next().stream().forEach(e -> log.warn(e.getMessage()));
                if (response.getInsertErrors().size() == recordsBuffer.size()) {
                    // All rows were rejected : there's an issue with schema ?
                    log.warn(records.get(0).getSchema().toString());
                    log.warn(tableSchema.toString());
                    // Let's show how the first record was handled.
                    log.warn(records.get(nbRecordsSent).toString());
                    log.warn(recordsBuffer.get(0).toString());
                }
            }

            nbRecordsSent += recordsBuffer.size();
        }

        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {

            try {
                this.recordWriter.add(records);
                this.recordWriter.flush();
            } catch (IOException exIO) {
                log.error(exIO.getMessage());
            }

            String sourceUri = "gs://" + configuration.getDataSet().getGsBucket() + "/" + blobName;
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());
            Job job = bigQuery.create(JobInfo.of(loadConfig));
            try {
                job = job.waitFor();
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
            }
            if (job.isDone()) {
                log.info("Avro from GCS successfully loaded in a table");
            } else {
                log.warn(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }

            storage.delete(blobInfo.getBlobId());
        }
    }

    @PreDestroy
    public void release() {
        try {
            if (this.recordWriter != null) {
                this.recordWriter.end();
            }
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }
    }

    private TableReference createTableReference() {
        TableReference table = new TableReference();
        table.setProjectId(connection.getProjectName());
        table.setDatasetId(configuration.getDataSet().getBqDataset());
        table.setTableId(configuration.getDataSet().getTableName());
        return table;
    }

}
