package io.tusk.stream.func;

import java.util.function.Predicate;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * The data filter class
 * 
 * 
 * @author kenny.li
 *
 * @param <Config>
 * @param <Context>
 */
public class DataFilterFunction<Config, Context> extends BaseOperation<Context> implements Filter<Context> {

    private static final long serialVersionUID = -8163034869041927892L;

    private Predicate<TupleEntry> input;

    public DataFilterFunction(Predicate<TupleEntry> input) {
        super();
        this.input = input;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall<Context> filterCall) {
        TupleEntry entry = filterCall.getArguments();
        return input.test(entry);
    }

}
