package io.tusk.stream.impl;

import io.tusk.stream.DataStream;
import io.tusk.stream.GroupByDataStream;
import io.tusk.stream.StreamData;

import java.net.URI;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Predicate;

import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * The GroupBy Stream which was created by the groupBy method the instance will inherent the existing class stream and
 * provide additional aggregation support
 * 
 * 
 * @author kenny.li
 *
 */
public class GroupByDataStreamImpl implements GroupByDataStream<StreamData> {

    private LinkedList<Pipe> pipes = new LinkedList<Pipe>();

    public LinkedList<Pipe> getPipes() {
        return this.pipes;
    }

    public void setPipes(LinkedList<Pipe> pipes) {
        this.pipes = pipes;
    }

    @Override
    public GroupByDataStream<StreamData> average(String newFieldName, String... fieldsName) {

        Fields fields = new Fields();

        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }
        Average average = new Average(new Fields(newFieldName));

        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), fields, average);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> count(String newFieldName, String... fieldsName) {

        Fields fields = new Fields();

        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }

        Count count = new Count(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), fields, count);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> max(String newFieldName, String... fieldsName) {
        Fields fields = new Fields();

        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }

        MaxValue max = new MaxValue(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), fields, max);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> min(String newFieldName, String... fieldsName) {
        Fields fields = new Fields();

        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }
        MinValue min = new MinValue(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), fields, min);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> sum(String newFieldName, String... fieldsName) {
        Fields fields = new Fields();

        for (String fieldName : fieldsName) {
            fields = fields.append(new Fields(fieldName));
        }
        Sum sum = new Sum(new Fields(newFieldName));
        LinkedList<Pipe> pipes = getPipes();
        Every every = new Every(pipes.getLast(), fields, sum);
        pipes.add(every);
        setPipes(pipes);
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> reduce(Function<StreamData, StreamData> func) {
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> groupBy(String... fields) {
        return null;
    }

    @Override
    public DataStream<StreamData> sample(double percentage) {
        return null;
    }

    @Override
    public DataStream<StreamData> head(long num) {
        return null;
    }

    @Override
    public DataStream<StreamData> process(Function<StreamData, StreamData> func) {
        return null;
    }


    @Override
    public DataStream<StreamData> distinct() {
        return null;
    }

    @Override
    public DataStream<StreamData> project(String... fields) {
        return null;
    }

    @Override
    public DataStream<StreamData> discard(String... fields) {
        return null;
    }

    @Override
    public DataStream<StreamData> rename(String name, String targetName) {
        return null;
    }

    @Override
    public DataStream<StreamData> mapTo(String sourceField, String newField, Function<String, String> function) {
        return null;
    }

    @Override
    public DataStream<StreamData> mapField(Function<String, String> function) {
        return null;
    }

    @Override
    public DataStream<StreamData> leftJoin(DataStream<StreamData> rightStream) {
        return null;
    }

    @Override
    public DataStream<StreamData> rightJoin(DataStream<StreamData> rightStream) {
        return null;
    }

    @Override
    public DataStream<StreamData> outerJoin(DataStream<StreamData> rightStream) {
        return null;
    }

    @Override
    public DataStream<StreamData> innerJoin(DataStream<StreamData> rightStream) {
        return null;
    }

    @Override
    public void writeTo(URI location, String delimitor) {

    }

    @Override
    public DataStream<StreamData> filter(Predicate<StreamData> predicate) {
        return null;
    }

    @Override
    public DataStream<StreamData> map(String fieldName, Function<String, String> mapper) {
        return null;
    }

    @Override
    public DataStream<StreamData> flatMap(Function<String, String> mapper) {
        return null;
    }

    @Override
    public DataStream<StreamData> debug() {
        return null;
    }
}
