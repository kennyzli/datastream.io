package org.datastream.stream.impl;

import java.util.LinkedList;
import java.util.function.Function;

import org.datastream.stream.GroupByDataStream;
import org.datastream.stream.StreamData;

import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class GroupByDataStreamImpl implements GroupByDataStream<StreamData> {

    private LinkedList<Pipe> pipes = new LinkedList<Pipe>();

    public LinkedList<Pipe> getPipes() {
        return this.pipes;
    }

    public void setPipes(LinkedList<Pipe> pipes) {
        this.pipes = pipes;
    }

    @Override
    public GroupByDataStream<StreamData> average(String newFieldName) {
        Average average = new Average(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), average);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> count(String newFieldName) {
        Count count = new Count(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), count);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> max(String newFieldName) {
        MaxValue max = new MaxValue(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), max);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> min(String newFieldName) {
        MinValue min = new MinValue(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), min);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> sum(String newFieldName) {
        Sum sum = new Sum(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), sum);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> reduce(Function<StreamData, StreamData> func) {
        // TODO Auto-generated method stub
        return null;
    }
}
