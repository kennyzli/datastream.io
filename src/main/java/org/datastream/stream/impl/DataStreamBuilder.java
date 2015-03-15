package org.datastream.stream.impl;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream.Builder;

import org.datastream.stream.DataStream;
import org.datastream.stream.StreamData;

import cascading.tap.Tap;

/**
 * The DataStream Builder class is the entry point class which place the factory role to construct the DataStreamBuilder
 * Implemention based on current Implementation
 * 
 * @author kenny.li
 *
 */
public class DataStreamBuilder implements Builder<StreamData> {
    /* Source tap eventually will be add to the stream as source */
    private List<Tap> sourceTaps = new LinkedList<Tap>();
    /* Target tag eventually will be added to the stream as sink */
    private List<Tap> targetTaps = new LinkedList<Tap>();

    protected String name;
    /**
     * Support the data type of the builder it can be CSV, TSV or other compressed table like files
     * 
     * @author kenny.li
     *
     */
    public enum BUILDER_TYPE {
        CSV_STREAM_BUILDER;
    }

    /**
     * Runtime mode indicat the class can be run locally or on distributed mode like Hadoop env or other env.
     * 
     * @author kenny.li
     *
     */
    public enum RUNTIME_MODE {
        LOCAL_MODE,
        HADOOP_MODE
    }

    private String fieldSep = "\t";
    private String lineSep = "\n";
    /*
     * The mode indicate the runtime mode of the application which should be run locally or distributed system like
     * Hadoop or AWS EMR job
     */
    private RUNTIME_MODE runtimeMode = RUNTIME_MODE.LOCAL_MODE;

    public final static DataStreamBuilder getBuilder(BUILDER_TYPE type, RUNTIME_MODE runtimeMode)
            throws DataStreamException {
        switch (type) {
        case CSV_STREAM_BUILDER:
            return new CSVDataStreamBuilderImpl(runtimeMode);
        }

        assert false : "Not reachable code";
        throw new DataStreamException("Unreachable code exception");
    }

    /**
     * Given the location of the data source, the builder need to be able to import the data from the location
     * 
     * @param location
     * @return
     */
    public DataStreamBuilder source(URI location) {
        throw new UnsupportedOperationException();
    }

    /**
     * The field need to be set so that the parser is able to parse the datasource in
     * 
     * @param sep
     *            by default it should be "Tab \t" if nothing get specified
     * @return
     */
    public DataStreamBuilder fieldSeparator(String sep) {
        this.setFieldSep(sep);
        return this;
    }

    /**
     * The line seperator which indicated the seperator, normaly it shoul be "\n" no need to reset it unless it is
     * necessary
     * 
     * @param sep
     * @return
     */
    public DataStreamBuilder lineSeparator(String sep) {
        this.setLineSep(sep);
        return this;
    }

    /**
     * The name of this workflow
     * 
     * 
     * @param name
     */
    public void name(String name) {
        this.name = name;
    }

    @Override
    public void accept(StreamData t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataStream<StreamData> build() {
        throw new UnsupportedOperationException();
    }

    public String getFieldSep() {
        return fieldSep;
    }

    protected void setFieldSep(String fieldSep) {
        this.fieldSep = fieldSep;
    }

    public String getLineSep() {
        return lineSep;
    }

    protected void setLineSep(String lineSep) {
        this.lineSep = lineSep;
    }

    public RUNTIME_MODE getRuntimeMode() {
        return runtimeMode;
    }

    public void setRuntimeMode(RUNTIME_MODE runtimeMode) {
        this.runtimeMode = runtimeMode;
    }

}
