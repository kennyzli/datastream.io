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
    GroupByDataStream<T> average(String agFieldName, String newFieldName);

    GroupByDataStream<T> count(String agFieldName, String newFieldName);

    GroupByDataStream<T> max(String agFieldName, String newFieldName);

    GroupByDataStream<T> min(String agFieldName, String newFieldName);

    GroupByDataStream<T> sum(String agFieldName, String newFieldName);

    GroupByDataStream<T> reduce(String agFieldName, Function<T, T> func);
}
