package org.datastream.stream.impl.local;

import java.util.LinkedList;
import java.util.function.Function;

import org.datastream.stream.GroupByDataStream;
import org.datastream.stream.StreamData;
import org.datastream.stream.impl.GroupByDataStreamImpl;

import cascading.pipe.Pipe;

import com.google.common.collect.Lists;

/**
 * The Local GroupBy DataStream
 * 
 * @author kenny.li
 *
 */
public class LocalGroupByDataStream extends LocalDataStream implements GroupByDataStream<StreamData> {
    // The real implementation
    private GroupByDataStreamImpl groupByStream = new GroupByDataStreamImpl();

    LocalGroupByDataStream(LocalDataStream stream) {
        LinkedList<Pipe> pipes = Lists.newLinkedList();
        pipes.addAll(stream.getPipes());
        setPipes(pipes);
    }

    @Override
    public GroupByDataStream<StreamData> average(String newFieldName) {
        groupByStream.setPipes(getPipes());
        groupByStream.average(newFieldName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> count(String newFieldName) {
        groupByStream.setPipes(getPipes());
        groupByStream.count(newFieldName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> max(String newFieldName) {
        groupByStream.setPipes(getPipes());
        groupByStream.max(newFieldName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> min(String newFieldName) {
        groupByStream.setPipes(getPipes());
        groupByStream.min(newFieldName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> sum(String newFieldName) {
        groupByStream.setPipes(getPipes());
        groupByStream.sum(newFieldName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> reduce(Function<StreamData, StreamData> func) {
        groupByStream.setPipes(getPipes());
        groupByStream.reduce(func);
        setPipes(groupByStream.getPipes());
        return this;
    }

}
