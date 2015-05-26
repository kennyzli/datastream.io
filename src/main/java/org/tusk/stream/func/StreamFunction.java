package org.tusk.stream.func;


import cascading.operation.Function;

/**
 * The stream function is the one which map the Stream function all the way to the cascading function
 * 
 * @author kenny.li
 *
 */
public interface StreamFunction<Context, T, R> extends Function<Context> {
    
    void setFunction(java.util.function.Function<T, R> func);
}
