package org.datastream.stream.impl;

import java.util.LinkedList;

import org.datastream.stream.StreamSource;

import cascading.pipe.Pipe;

public class LocalDataStream extends AbstractDataStream {

    private String name;
    private LocalStreamSource source;

    public LocalDataStream() {

    }

    public LocalDataStream(String name, LocalStreamSource dataSource) {
        assert dataSource != null;
        this.name = name;

        this.source = dataSource;
        setSourcePipe(new Pipe(name + ":source"));

        LinkedList<Pipe> list = getPipes();
        list.add(getSourcePipe());
        setPipes(list);

    }

    @Override
    protected StreamSource getStreamSource() {
        return source;
    }
}
