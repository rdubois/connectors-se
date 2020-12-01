/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.BigQueryTestUtil;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.Environments;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents(value = "org.talend.components.bigquery")
public class BigQueryOutputITCase {

    public static final String COMMA_DELIMITER = ";";

    @Service
    public RecordBuilderFactory rbf;

    @Service
    private RecordIORepository ioRepository;

    @Injected
    private BaseComponentsHandler handler;

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

    }

    @Test
    public void fillData() {
        int batchSize = 1000;

        BigQueryConnection connection = BigQueryTestUtil.getConnection();

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("onimych");
        dataset.setTableName("person");

        BigQueryOutputConfig config = new BigQueryOutputConfig();
        config.setDataSet(dataset);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        configURI += "&$configuration.$maxBatchSize=" + batchSize;

//        final int nbrecords = 1_000;

//        Iterable<Record> inputData = new RandomDataGenerator(nbrecords, rbf);

        List<Record> inputData = new ArrayList<>();

        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("field0").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field1").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field2").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field3").withType(Schema.Type.STRING).build())
                .build();

        fillInputData(inputData, schema);


        COMPONENTS.setInputData(inputData);

        long start = System.currentTimeMillis();

        Job.components().component("source", "test://emitter").component("output", "BigQuery://BigQueryOutput?" + configURI)
                .connections().from("source").to("output").build().run();

        long end =  System.currentTimeMillis() - start;

        System.out.println("Append job run took " + end + " ms");

    }

    @Test
    public void run() {
        int batchSize = 1000;

        BigQueryConnection connection = BigQueryTestUtil.getConnection();

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("onimych");
        dataset.setTableName("person");

        BigQueryOutputConfig config = new BigQueryOutputConfig();
        config.setDataSet(dataset);
        config.setTableOperation(BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        configURI += "&$configuration.$maxBatchSize=" + batchSize;

        List<Record> inputData = new ArrayList<>();

        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("field0").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field1").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field2").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field3").withType(Schema.Type.STRING).build())
                .build();


        fillInputData(inputData, schema);


        COMPONENTS.setInputData(inputData);

        long start = System.currentTimeMillis();


        Job.components().component("source", "test://emitter").component("output", "BigQuery://BigQueryOutput?" + configURI)
                .connections().from("source").to("output").build().run();

        long end =  System.currentTimeMillis() - start;

        System.out.println("CreateIfNotExist job run took " + end + " ms");


    }

    @Test
    public void overrideData() {
        int batchSize = 1000;

        BigQueryConnection connection = BigQueryTestUtil.getConnection();

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("onimych");
        dataset.setTableName("person");
        dataset.setGsBucket("onimych");

        BigQueryOutputConfig config = new BigQueryOutputConfig();
        config.setDataSet(dataset);
        config.setTableOperation(BigQueryOutputConfig.TableOperation.TRUNCATE);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        configURI += "&$configuration.$maxBatchSize=" + batchSize;

        List<Record> inputData = new ArrayList<>();

        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("field0").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field1").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field2").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("field3").withType(Schema.Type.STRING).build())
                .build();

        fillInputData(inputData, schema);

        COMPONENTS.setInputData(inputData);

        long start = System.currentTimeMillis();

        Job.components().component("source", "test://emitter").component("output", "BigQuery://BigQueryOutput?" + configURI)
                .connections().from("source").to("output").build().run();

        long end =  System.currentTimeMillis() - start;

        System.out.println("Truncate job run took " + end + " ms");

    }

    private void fillInputData(List<Record> inputData, Schema schema) {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("D:/Talend/Jira/TDI-44723/person_100000.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(COMMA_DELIMITER);
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (List<String> record : records) {
            inputData.add(rbf.newRecordBuilder(schema)
                    .withString("field0", record.get(0))
                    .withString("field1", record.get(1))
                    .withString("field2", record.get(2))
                    .withString("field3", record.get(3))
                    .build());
        }
        String s = "ha";
    }
/*

    private List<String> getRecordFromLine(String line) {
        List<String> values = new ArrayList<String>();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(COMMA_DELIMITER);
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next());
            }
        }
        return values;
    }
*/
}
