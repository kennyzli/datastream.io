package org.datastream.stream;

import java.net.URI;
import java.util.function.Function;
import java.util.stream.Stream;

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
public interface DataStream<T extends StreamData> extends Stream<T> {
    /**
     * Return the Stream<T> contains the size of the elements
     * 
     * @param num
     *            that the stream need to return
     * @return the Stream contains the size of elements
     */
    public DataStream<T> head(long num);

    /**
     * Return the tails of the elements
     * 
     * @param the
     *            number of the tails elements
     * @return the Stream elements contains the number of the elements
     */
    public DataStream<T> tail(long num);

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
     * Add additional fiels to the Stream columns
     * 
     * @param function
     *            the function which take the input and return the list of new fields name and values
     * @return
     */
    public DataStream<T> addFields(Function<T, String[]> function, String... fields);

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
    public DataStream<T> leftJoin(Stream<T> rightStream);

    /**
     * Right join the stream and return the new stream with the element type
     * 
     * @param rightStream
     * @return new Stream
     */
    public DataStream<T> rightJoin(Stream<T> rightStream);
    
    
    /**
     * outter join the stream and return the new stream with the element type <T>
     * 
     * @return
     */
    public DataStream<T> outerJoin(Stream<T> rightStream);

    /*
     * 
     * Right join the stream and return the elements
     */
    public DataStream<T> innerJoin(Stream<T> rightStream);

    /**
     * Write the data to the output location
     * 
     * @param location
     *            output URI location
     */
    public void writeTo(URI location);
}
