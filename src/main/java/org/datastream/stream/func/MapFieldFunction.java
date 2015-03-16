package org.datastream.stream.func;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
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
    private String sourceFieldName;
    private String newFieldName;

    public MapFieldFunction(String sourceField, String newField) {
        this.sourceFieldName = sourceField;
        this.newFieldName = newField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
        Fields fields = functionCall.getDeclaredFields();
        TupleEntry entry = functionCall.getArguments();

        String[] newFields = new String[fields.size()];

        int i = 0;
        for (Iterator<String> iterFields = fields.iterator(); iterFields.hasNext(); i++) {
            String field = iterFields.next();

            if (!sourceFieldName.equals(field)) {
                newFields[i] = field;
            }else{
                newFields[i] = newFieldName;
            }

        }
        Fields outGoingFields = new Fields(newFields);
        TupleEntry outGoingTupleEntry = new TupleEntry(outGoingFields);
        Tuple ctuple = entry.getTupleCopy();
        outGoingTupleEntry.setTuple(ctuple);
        outGoingTupleEntry.setString(newFieldName, getFunction().apply(entry.getString(sourceFieldName)));
        // outGoingTupleEntry.setString(newFieldName, "USA");

        TupleEntryCollector collector = functionCall.getOutputCollector();
        collector.setFields(outGoingFields);
        collector.add(outGoingTupleEntry);

    }
}
