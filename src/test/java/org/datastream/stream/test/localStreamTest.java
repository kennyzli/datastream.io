package org.datastream.stream.test;

import static junit.framework.Assert.assertNotNull;

import java.net.URI;
import java.net.URISyntaxException;

import org.datastream.stream.DataStream;
import org.datastream.stream.StreamData;
import org.datastream.stream.impl.DataStreamBuilder;
import org.datastream.stream.impl.DataStreamBuilder.BUILDER_TYPE;
import org.datastream.stream.impl.DataStreamBuilder.RUNTIME_MODE;
import org.junit.Test;

public class localStreamTest {
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
        stream.writeTo(new URI("data/output/data.dat"));
    }

    @Test
    public void testDataStreamMapWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.mapTo("county", "OtherCountry", x -> x + ":newData").writeTo(new URI("data/output/mapped.dat"));
    }

    @Test
    public void testDataStreamtDinstinctWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.distinct().writeTo(new URI("data/output/distinct.dat"));
    }

    @Test
    public void testDataStreamProjectWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.project("county").writeTo(new URI("data/output/project.csv"));
    }

    @Test
    public void testDataStreamDiscardWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.discard("county", "statecode").writeTo(new URI("data/output/discard.csv"));
    }

    @Test
    public void testDataStreamSortedWithLocalMode() throws URISyntaxException {

        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.sorted().writeTo(new URI("data/output/sorted.dat"));
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
                .writeTo(new URI("data/output/sorted.csv"));
    }

}
