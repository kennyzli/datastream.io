package org.datastream.stream;

import java.net.URI;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The DataStream is the class which extends the Stream interface and provide additional data streaming functions for
 * big data operation and operations across the different stream
 * 
 * 
 * @author kenny.li
 * @email kenny.zlee@gmail.com
 *
 * @param <T>
 */
public interface DataStream<T> {
    
    /**
     * Group by data fields list
     * 
     * 
     * @param fields
     * @return
     */
    public GroupByDataStream<T> groupBy(String... fields);

    /**
     * The sample percentage
     * 
     * 
     * @param percentage
     * @return
     */
    public DataStream<T> sample(double percentage);

    /**
     * The head of number of records
     * 
     * @param num
     * @return
     */
    public DataStream<T> head(long num);

    /**
     * The free style data which will accept all the line records and return the output
     * 
     * 
     * @param data
     * @return
     */
    public DataStream<T> process(Function<T, T> func);

    /**
     * make the distinct public and chanage the datatype to be DataStream
     * 
     * 
     */
    public DataStream<T> distinct();

    /**
     * project the columns
     * 
     * @param project
     *            the fields that want to keep
     * @return
     */
    public DataStream<T> project(String... fields);

    /**
     * Discard the columns
     * 
     * 
     * @param fields
     *            the fields which will be discard
     * @return
     */
    public DataStream<T> discard(String... fields);

    /**
     * rename the column
     * 
     * @param function
     *            the input name is a string and output name of the columns is also a string
     * @return
     */
    public DataStream<T> rename(String name, String targetName);

    /**
     * Map the column value to a new column this function is different from the traditional map function
     * 
     * @param function
     * @return
     */
    public DataStream<T> mapTo(String sourceField, String newField, Function<String, String> function);

    /**
     * Map the field with new value
     * 
     * @param function
     * @return
     */
    public DataStream<T> mapField(Function<String, String> function);

    /**
     * Left join the stream and return the elements
     * 
     * @param rightStream
     * @return new Stream
     */
    public DataStream<T> leftJoin(DataStream<T> rightStream);

    /**
     * Right join the stream and return the new stream with the element type
     * 
     * @param rightStream
     * @return new Stream
     */
    public DataStream<T> rightJoin(DataStream<T> rightStream);

    /**
     * outer join the stream and return the new stream with the element type <T>
     * 
     * @return
     */
    public DataStream<T> outerJoin(DataStream<T> rightStream);

    /*
     * 
     * Right join the stream and return the elements
     */
    public DataStream<T> innerJoin(DataStream<T> rightStream);

    /**
     * Write the data to the output location
     * 
     * @param location
     *            output URI location
     * @param delimitor
     *            The delimiter of each record
     */
    public void writeTo(URI location, String delimitor);

    /**
     * filter out the field
     * 
     * @param predicate
     * @return
     */
    DataStream<T> filter(Predicate<T> predicate);

    /**
     * map the field to the other value
     * 
     * @param mapper
     * @return
     */
    DataStream<T> map(String fieldName, Function<String, String> mapper);

    /**
     * flat map the data
     * 
     * @param mapper
     * @return
     */
    DataStream<T> flatMap(Function<String, String> mapper);

    /**
     * debug the stream content
     * 
     * @return
     */
    DataStream<T> debug();

}
