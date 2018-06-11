package org.talend.components.elasticsearch;

import static org.talend.sdk.component.api.component.Icon.IconType.ELASTIC;

import java.io.StringReader;
import java.util.stream.Stream;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;

import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.meta.Documentation;

@Version
@Icon(ELASTIC)
@PartitionMapper(name = "ElasticsearchInput")
@Documentation("This component reads data from Elasticsearch.")
public class ElasticsearchInput extends PTransform<PBegin, PCollection<JsonObject>> {

    private final ElasticsearchInputDataSet configuration;

    private final JsonReaderFactory readerFactory;

    public ElasticsearchInput(@Option("configuration") final ElasticsearchInputDataSet configuration,
            final JsonReaderFactory readerFactory) {
        this.configuration = configuration;
        this.readerFactory = readerFactory;
    }

    @Override
    public PCollection<JsonObject> expand(final PBegin input) {
        return input.apply(configure()).apply("StringToJson", ParDo.of(new DoFn<String, JsonObject>() {

            @ProcessElement
            public void processElement(final ProcessContext c) {
                c.output(readerFactory.createReader(new StringReader(c.element())).readObject());
            }
        }));
    }

    private ElasticsearchIO.Read configure() {
        ElasticsearchIO.Read current = ElasticsearchIO.read()
                .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(
                        Stream.of(configuration.getDatastore().getNodes().split(",")).map(String::trim).toArray(String[]::new),
                        configuration.getIndex(), configuration.getType()));
        if (configuration.getQuery() != null && !configuration.getQuery().isEmpty()) {
            current = current.withQuery(configuration.getQuery());
        }
        // todo: keep alive and batchsize
        return current;
    }
}
