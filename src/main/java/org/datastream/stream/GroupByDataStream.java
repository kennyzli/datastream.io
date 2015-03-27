package org.datastream.stream;

import java.util.function.Function;

/**
 * The stream which should be created by Data Stream groupBy method
 * 
 * 
 * @author kenny.li
 *
 * @param <T>
 *            the template field which is the record type of the Group DataStream
 */
public interface GroupByDataStream<T> extends DataStream<T> {
    /**
     * predefined aggregation method which calculate the average of the specified field
     * 
     * 
     * @param newFieldName
     * @return
     */
    GroupByDataStream<T> average(String newFieldName, String... fieldsName);

    /**
     * predefined aggregation method which calculate the count of the specified field.
     * 
     * @param newFieldName
     * @return
     */
    GroupByDataStream<T> count(String newFieldName, String... fieldsName);

    /**
     * predefined aggregation method which calculate the max value of the specified field
     * 
     * @param newFieldName
     * @return
     */
    GroupByDataStream<T> max(String newFieldName, String... fieldsName);

    /**
     * predefined aggregation method which calculate the min value of the specified field
     * 
     * @param newFieldName
     * @return
     */
    GroupByDataStream<T> min(String newFieldName, String... fieldsName);

    /**
     * predefined aggregation method which calculate the sum value of the specified field
     * 
     * @param newFieldName
     * @return
     */
    GroupByDataStream<T> sum(String newFieldName, String... fieldsName);

    /**
     * customized reduced method which calculate the 2 continuous field
     * 
     * @param func
     * @return
     */
    GroupByDataStream<T> reduce(Function<T, T> func);
}
