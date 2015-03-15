package org.datastream.stream.func;

import java.util.function.Function;

public abstract class AbstractFieldsStreamFunction<Context, T, R> implements StreamFunction<Context, T, R> {
    private Function<T, R> func;

    @Override
    public void setFunction(Function<T, R> func) {
        this.func = func;
    }

    protected Function<T, R> getFunction() {
        return func;
    }

}
