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
    public void testDataStreaMapWithLocalMode() throws URISyntaxException {
        DataStreamBuilder builder = DataStreamBuilder.getBuilder(BUILDER_TYPE.CSV_STREAM_BUILDER,
                RUNTIME_MODE.LOCAL_MODE);
        DataStream<StreamData> stream = builder.source(new URI("data/input/sample.csv")).build();
        stream.mapTo("county", "OtherCountry", x -> x + ":newData").writeTo(new URI("data/output/mapped.dat"));
    }
}
