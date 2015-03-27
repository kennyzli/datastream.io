package org.datastream.stream.impl;

import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;

import org.datastream.stream.DataStream;
import org.datastream.stream.StreamData;
import org.datastream.stream.StreamSource;
import org.datastream.stream.func.DataFilterFunction;
import org.datastream.stream.func.MapFieldFunction;
import org.datastream.stream.func.StreamDataFunction;
import org.datastream.stream.impl.local.LocalStreamSource;

import cascading.flow.FlowDef;
import cascading.operation.DebugLevel;
import cascading.operation.filter.Limit;
import cascading.operation.filter.Sample;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

import com.google.common.collect.Lists;

/**
 * The local csv dataStream implementation which is able to provide the solid implementation for the client
 * 
 * 
 * @author kenny.li
 *
 */
public abstract class AbstractDataStreamImpl implements DataStream<StreamData> {
    private Pipe sourcePipe;
    private FlowDef flowDef = new FlowDef();

    private LinkedList<Pipe> pipes = new LinkedList<Pipe>();

    public AbstractDataStreamImpl() {

    }

    public AbstractDataStreamImpl(AbstractDataStreamImpl stream) {
        LinkedList<Pipe> pipes = Lists.newLinkedList();
        pipes.addAll(stream.getPipes());
        setPipes(pipes);
        setSourcePipe(stream.getSourcePipe());
        setStreamSource(stream.getStreamSource());
    }

    abstract protected StreamSource getStreamSource();

    abstract protected void setStreamSource(StreamSource source);

    AbstractDataStreamImpl(String name, LocalStreamSource dataSource) {

    }

    public LinkedList<Pipe> getPipes() {
        return this.pipes;
    }

    public void setPipes(LinkedList<Pipe> pipes) {
        this.pipes = pipes;
    }


    @Override
    public DataStream<StreamData> filter(Predicate<StreamData> predicate) {
        pipes.add(new Each(pipes.getLast(), new DataFilterFunction(predicate)));
        return this;
    }

    @Override
    public DataStream<StreamData> map(String fieldName, Function<String, String> mapper) {
        return mapTo(fieldName, fieldName, mapper);
    }

    @Override
    public DataStream<StreamData> flatMap(Function<String, String> mapper) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public DataStream<StreamData> distinct() {
        Unique unique = new Unique(pipes.getLast(), Fields.ALL);
        pipes.push(unique);
        return this;
    }


    @Override
    public DataStream<StreamData> project(String... fieldNames) {
        Fields fields = new Fields();
        for (String fieldName : fieldNames) {
            fields = fields.append(new Fields(fieldName));
        }
        Retain retain = new Retain(pipes.getLast(), fields);
        pipes.add(retain);
        return this;
    }

    @Override
    public DataStream<StreamData> discard(String... fieldsName) {
        Fields fields = new Fields();
        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }
        Discard discard = new Discard(pipes.getLast(), fields);
        pipes.add(discard);
        return this;
    }

    @Override
    public DataStream<StreamData> rename(String name, String targetName) {
        pipes.add(new Rename(pipes.getLast(), new Fields(name), new Fields(targetName)));
        return this;
    }

    @Override
    public DataStream<StreamData> mapTo(String sourceField, String newField, Function<String, String> function) {

        MapFieldFunction func = new MapFieldFunction(sourceField, newField);
        func.setFunction(function);
        Pipe pipe = new Each(pipes.getLast(), Fields.ALL, func);
        pipes.add(pipe);
        return this;
    }

    @Override
    public DataStream<StreamData> mapField(Function<String, String> function) {
        return null;
    }

    @Override
    public DataStream<StreamData> leftJoin(DataStream<StreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<StreamData> rightJoin(DataStream<StreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<StreamData> innerJoin(DataStream<StreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<StreamData> outerJoin(DataStream<StreamData> rightStream) {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public DataStream<StreamData> sorted(String... fields) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStream<StreamData> debug() {
        getFlowDef().setDebugLevel(DebugLevel.VERBOSE);
        return this;
    }

    @Override
    public DataStream<StreamData> process(Function<StreamData, StreamData> func) {
        StreamDataFunction newFunc = new StreamDataFunction();
        newFunc.setFunction(func);
        pipes.add(new Each(pipes.getLast(), newFunc));
        return this;
    }

    @Override
    public DataStream<StreamData> head(long num) {
        Limit limit = new Limit(num);
        Each each = new Each(pipes.getLast(), limit);
        pipes.add(each);
        return this;
    }

    @Override
    public DataStream<StreamData> sample(double percentage) {
        Sample sample = new Sample(percentage);
        Each each = new Each(pipes.getLast(), sample);
        pipes.add(each);
        return this;
    }



    public Pipe getSourcePipe() {
        return sourcePipe;
    }

    protected void setSourcePipe(Pipe sourcePipe) {
        this.sourcePipe = sourcePipe;
    }

    protected FlowDef getFlowDef() {
        return flowDef;
    }

    protected void setFlowDef(FlowDef def) {
        flowDef = def;
    }


}
