package org.datastream.stream.func;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * The function class which map the map specified field to the target field
 * 
 * @author kenny.li
 *
 * @param <Context>
 */
public class MapFieldFunction<Context> extends DataFunction<Context, String, String> {
    private String sourceField;
    private String newField;

    public MapFieldFunction(String sourceField, String newField) {
        this.sourceField = sourceField;
        this.newField = newField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
        Fields fields = functionCall.getDeclaredFields();
        TupleEntry entry = functionCall.getArguments();
        // setup the logic

        TupleEntryCollector collector = functionCall.getOutputCollector();
        collector.setFields(fields);
        collector.add(entry);

    }
}
