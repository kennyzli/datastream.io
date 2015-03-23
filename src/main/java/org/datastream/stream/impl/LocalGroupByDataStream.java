package org.datastream.stream.impl;

import java.util.LinkedList;
import java.util.function.Function;

import org.datastream.stream.GroupByDataStream;
import org.datastream.stream.StreamData;

import cascading.pipe.Pipe;

import com.google.common.collect.Lists;

public class LocalGroupByDataStream extends LocalCSVDataStream implements GroupByDataStream<StreamData> {

    LocalGroupByDataStream(LocalCSVDataStream stream) {
        LinkedList<Pipe> pipes = Lists.newLinkedList();
        pipes.addAll(stream.getPipes());
        setPipes(pipes);
    }

    @Override
    public GroupByDataStream<StreamData> average(String agFieldName, String newFieldName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> count(String agFieldName, String newFieldName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> max(String agFieldName, String newFieldName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> min(String agFieldName, String newFieldName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> sum(String agFieldName, String newFieldName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GroupByDataStream<StreamData> reduce(String agFieldName, Function<StreamData, StreamData> func) {
        // TODO Auto-generated method stub
        return null;
    }

}
