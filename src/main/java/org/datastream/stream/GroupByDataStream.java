package org.datastream.stream;

import java.util.function.Function;

/**
 * The stream which should be created by Data Stream groupBy method
 * 
 * 
 * @author kenny.li
 *
 * @param <T>
 */
public interface GroupByDataStream<T> extends DataStream<T> {
    GroupByDataStream<T> average(String newFieldName);

    GroupByDataStream<T> count(String newFieldName);

    GroupByDataStream<T> max(String newFieldName);

    GroupByDataStream<T> min(String newFieldName);

    GroupByDataStream<T> sum(String newFieldName);

    GroupByDataStream<T> reduce(Function<T, T> func);
}
