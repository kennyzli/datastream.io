package io.tusk.stream.test;

import static junit.framework.Assert.assertNotNull;
import io.tusk.stream.DataStream;
import io.tusk.stream.StreamData;
import io.tusk.stream.impl.DataStreamBuilder;
import io.tusk.stream.impl.DataStreamBuilder.BUILDER_TYPE;
import io.tusk.stream.impl.DataStreamBuilder.RUNTIME_MODE;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class LocalStreamTest {
    @Test
    public void TestDataStreamBuilderCreation() {
        assertNotNull(DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER, RUNTIME_MODE.LOCAL_MODE));
    }

    @Test
    public void testDataStreamBuilder() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        assertNotNull(builder.source(new URI("data/data.dat")).fieldSeparator(",").build());
    }

    @Test
    public void testDataStreamCopyWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.writeTo(new URI("data/output/data.dat"), ",");
    }

    @Test
    public void testDataStreamMapWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.mapTo("county", "OtherCountry", x -> x + ":newData").writeTo(new URI("data/output/mapped.dat"), ",");
    }

    @Test
    public void testDataStreamtDinstinctWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.distinct().writeTo(new URI("data/output/distinct.dat"), ",");
    }

    @Test
    public void testDataStreamProjectWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.project("county").writeTo(new URI("data/output/project.csv"), ",");
    }

    @Test
    public void testDataStreamDiscardWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.discard("county", "statecode").writeTo(new URI("data/output/discard.csv"), ",");
    }

    @Test
    public void testDataStreamWithGroupByCount() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.groupBy("county").count("count", "point_granularity")
                .writeTo(new URI("data/output/count_groupby.csv"), ",");
    }

    @Test
    public void testDataStreamWithGroupByAvg() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.groupBy("county").average("average", "point_granularity")
                .writeTo(new URI("data/output/average_groupby.csv"), ",");
    }

    @Test
    public void testDataStreamWithGroupByMax() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.groupBy("county").max("max", "point_granularity").writeTo(new URI("data/output/max_groupby.csv"), ",");
    }

    @Test
    public void testDataStreamWithGroupByMin() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.groupBy("county").min("min", "point_granularity").writeTo(new URI("data/output/min_groupby.csv"), ",");
    }

    @Test
    public void testDataStreamWithGroupBySum() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.groupBy("county").sum("sum", "point_granularity").writeTo(new URI("data/output/sum_groupby.csv"), ",");
    }

    @Test
    public void testDataStreamWithLeftJoinLocalMode() throws URISyntaxException {

    }

    @Test
    public void testDataStreamWithInnerJoinLocalMode() throws URISyntaxException {

    }

    @Test
    public void testDataStreamWithOutterJoinLocalMode() throws URISyntaxException {

    }

    @Test
    public void testDataStreamWithRightJoinLocalMode() throws URISyntaxException {

    }

    @Test
    public void testDataStreamDebugLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.debug()
                .writeTo(new URI("data/output/sorted.csv"), ",");
    }

}
