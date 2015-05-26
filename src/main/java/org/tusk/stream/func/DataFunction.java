package org.tusk.stream.func;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;


public class DataFunction<Context, T, R> extends AbstractFieldsStreamFunction<Context, T, R> {

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {

    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        // TODO Auto-generated method stub

    }

    @Override
    public void flush(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        // TODO Auto-generated method stub

    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<Context> operationCall) {
        // TODO Auto-generated method stub

    }

    @Override
    public Fields getFieldDeclaration() {
        return Fields.ALL;
    }

    @Override
    public int getNumArgs() {
        return 0;
    }

    @Override
    public boolean isSafe() {
        // TODO Auto-generated method stub
        return false;
    }

}
