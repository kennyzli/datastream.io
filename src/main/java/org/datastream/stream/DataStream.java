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
     * Return the tails of the elements
     * 
     * @param the
     *            number of the tails elements
     * @return the Stream elements contains the number of the elements
     */
    public DataStream<T> tail(long num);

    /**
     * sort the stream with the compared function
     * 
     */
    public DataStream<T> sorted(String... fields);

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
     * outter join the stream and return the new stream with the element type <T>
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
     */
    public void writeTo(URI location);

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
     * reduce the records
     * 
     * @param func
     * @return
     */
    DataStream<T> reduce(Function<String, String> func);

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
