package org.datastream.stream.impl;

import java.net.URI;

import org.datastream.stream.DataStream;
import org.datastream.stream.StreamData;

/**
 * The CSV Data Stream builder Impl which take the CSV stream source
 * 
 * @author kenny.li
 *
 */
class CSVDataStreamBuilderImpl extends DataStreamBuilder {
    private RUNTIME_MODE runtime = RUNTIME_MODE.LOCAL_MODE;
    private URI location;

    /**
     * The CSV Data Stream builder is the one which only should be initiated by the DataStreamBuilder class This class
     * should not be exposed to outside the world
     */
    CSVDataStreamBuilderImpl(RUNTIME_MODE runtimeMode) {
        this.runtime = runtimeMode;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataStream build() {
        if (location == null) {
            throw new DataStreamException("The locaiton was not set yet");
        }
        switch (runtime) {
        case LOCAL_MODE:
            LocalCSVStreamSource source = new LocalCSVStreamSource(location);
            LocalCSVDataStream stream = new LocalCSVDataStream(name, source);
            return stream;
        case HADOOP_MODE:
            // TODO: the hadoop mode need to be enhanced later on
            return null;
        }
        assert false : "This should be unreached code.";
        return null;
    }

    @Override
    public void accept(StreamData data) {
        super.accept(data);
    }

    @Override
    public DataStreamBuilder source(URI location) {
        this.location = location;
        return this;
    }

}
