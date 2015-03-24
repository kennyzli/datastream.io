package org.datastream.stream.impl;

import java.net.URI;

import org.datastream.stream.DataStream;
import org.datastream.stream.exp.DataStreamException;
import org.datastream.stream.impl.hadoop.HadoopDataStream;
import org.datastream.stream.impl.hadoop.HadoopStreamSource;
import org.datastream.stream.impl.local.LocalDataStream;
import org.datastream.stream.impl.local.LocalStreamSource;

/**
 * The CSV Data Stream builder Impl which take the CSV stream source
 * 
 * @author kenny.li
 *
 */
class DataStreamBuilderImpl extends DataStreamBuilder {
    private RUNTIME_MODE runtime = RUNTIME_MODE.LOCAL_MODE;
    private URI location;

    /**
     * The CSV Data Stream builder is the one which only should be initiated by the DataStreamBuilder class This class
     * should not be exposed to outside the world
     */
    DataStreamBuilderImpl(RUNTIME_MODE runtimeMode) {
        this.runtime = runtimeMode;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataStream build() {
        if (location == null) {
            throw new DataStreamException("The locaiton can't be empty");
        }
        switch (runtime) {
        case LOCAL_MODE:
            LocalStreamSource lsource = new LocalStreamSource(location);
            LocalDataStream lstream = new LocalDataStream(name, lsource);
            return lstream;
        case HADOOP_MODE:
            HadoopStreamSource hsource = new HadoopStreamSource(location);
            HadoopDataStream hstream = new HadoopDataStream(name, hsource);
            return hstream;
        }
        assert false : "This should be unreachable code.";
        return null;
    }


    @Override
    public DataStreamBuilder source(URI location) {
        this.location = location;
        return this;
    }

}
