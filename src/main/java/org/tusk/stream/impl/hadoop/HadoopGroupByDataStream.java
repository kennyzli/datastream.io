package org.tusk.stream.impl.hadoop;

import java.util.function.Function;

import org.tusk.stream.DataStream;
import org.tusk.stream.GroupByDataStream;
import org.tusk.stream.StreamData;
import org.tusk.stream.impl.GroupByDataStreamImpl;

/**
 * The Local GroupBy DataStream
 * 
 * @author kenny.li
 *
 */
public class HadoopGroupByDataStream extends HadoopDataStream implements GroupByDataStream<StreamData>,
        DataStream<StreamData> {
    // The real implementation
    private GroupByDataStreamImpl groupByStream = new GroupByDataStreamImpl();

    HadoopGroupByDataStream(HadoopDataStream stream) {
        super(stream);
    }

    @Override
    public GroupByDataStream<StreamData> average(String newFieldName, String... fieldsName) {
        groupByStream.setPipes(getPipes());
        groupByStream.average(newFieldName, fieldsName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> count(String newFieldName, String... fieldsName) {
        groupByStream.setPipes(getPipes());
        groupByStream.count(newFieldName, fieldsName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> max(String newFieldName, String... fieldsName) {
        groupByStream.setPipes(getPipes());
        groupByStream.max(newFieldName, fieldsName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> min(String newFieldName, String... fieldsName) {
        groupByStream.setPipes(getPipes());
        groupByStream.min(newFieldName, fieldsName);
        setPipes(groupByStream.getPipes());
        return this;
    }

    @Override
    public GroupByDataStream<StreamData> sum(String newFieldName, String... fieldsName) {
        groupByStream.setPipes(getPipes());
        groupByStream.sum(newFieldName, fieldsName);
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
