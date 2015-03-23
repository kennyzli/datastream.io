package org.datastream.stream.impl;

import java.net.URI;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;

import org.datastream.stream.DataStream;
import org.datastream.stream.func.DataFilter;
import org.datastream.stream.func.MapFieldFunction;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * The local csv dataStream implementation which is able to provide the solid implementation for the client
 * 
 * 
 * @author kenny.li
 *
 */
public class LocalCSVDataStream implements DataStream<CSVStreamData> {
    private LocalCSVStreamSource source;
    private Pipe sourcePipe;
    private FlowDef flowDef = new FlowDef();

    private LinkedList<Pipe> pipes = new LinkedList<Pipe>();
    private String name;

    LocalCSVDataStream(String name, LocalCSVStreamSource dataSource) {
        assert dataSource != null;
        this.name = name;

        this.source = dataSource;
        sourcePipe = new Pipe(name + ":source");
        pipes.push(sourcePipe);

    }

    @Override
    public DataStream<CSVStreamData> filter(Predicate<CSVStreamData> predicate) {
        pipes.push(new Each(pipes.getLast(), new DataFilter(predicate)));
        return this;
    }

    @Override
    public DataStream<CSVStreamData> map(String fieldName, Function<String, String> mapper) {
        return mapTo(fieldName, fieldName, mapper);
    }

    @Override
    public DataStream<CSVStreamData> flatMap(Function<String, String> mapper) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public DataStream<CSVStreamData> distinct() {
        Unique unique = new Unique(pipes.getLast(), Fields.ALL);
        pipes.push(unique);
        return this;
    }



    @Override
    public DataStream<CSVStreamData> reduce(Function<String, String> func) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public DataStream<CSVStreamData> tail(long num) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<CSVStreamData> project(String... fieldNames) {
        Fields fields = new Fields();
        for (String fieldName : fieldNames) {
            fields = fields.append(new Fields(fieldName));
        }
        Retain retain = new Retain(pipes.getLast(), fields);
        pipes.add(retain);
        return this;
    }

    @Override
    public DataStream<CSVStreamData> discard(String... fieldsName) {
        Fields fields = new Fields();
        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }
        Discard discard = new Discard(pipes.getLast(), fields);
        pipes.add(discard);
        return this;
    }

    @Override
    public DataStream<CSVStreamData> rename(String name, String targetName) {
        pipes.add(new Rename(pipes.getLast(), new Fields(name), new Fields(targetName)));
        return this;
    }

    @Override
    public DataStream<CSVStreamData> mapTo(String sourceField, String newField, Function<String, String> function) {

        MapFieldFunction func = new MapFieldFunction(sourceField, newField);
        func.setFunction(function);
        Pipe pipe = new Each(pipes.getLast(), Fields.ALL, func);
        pipes.add(pipe);
        return this;
    }

    @Override
    public DataStream<CSVStreamData> mapField(Function<String, String> function) {
        return null;
    }

    @Override
    public DataStream<CSVStreamData> leftJoin(DataStream<CSVStreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<CSVStreamData> rightJoin(DataStream<CSVStreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<CSVStreamData> innerJoin(DataStream<CSVStreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<CSVStreamData> outerJoin(DataStream<CSVStreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void writeTo(URI location) {
        Scheme scheme = new TextDelimited(true, ",");
        Tap sinkTap = new FileTap(scheme, location.getPath());
        flowDef = flowDef.addSource(sourcePipe, source.getSourceTap())
                .addTailSink(pipes.getLast(), sinkTap).setName(name);
        assert flowDef != null;
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }

    @Override
    public DataStream<CSVStreamData> sorted(String... fields) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<CSVStreamData> debug() {
        flowDef.setDebugLevel(DebugLevel.VERBOSE);
        return this;
    }

}
