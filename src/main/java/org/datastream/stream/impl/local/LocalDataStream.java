package org.datastream.stream.impl.local;

import java.net.URI;
import java.util.LinkedList;

import org.datastream.stream.GroupByDataStream;
import org.datastream.stream.StreamData;
import org.datastream.stream.StreamSource;
import org.datastream.stream.impl.AbstractDataStreamImpl;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class LocalDataStream extends AbstractDataStreamImpl {

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

    @Override
    public GroupByDataStream<StreamData> groupBy(String... fields) {
        Fields field = new Fields();
        for (String fieldName : fields) {
            field = field.append(new Fields(fieldName));
        }
        LinkedList<Pipe> pipes = new LinkedList<Pipe>();
        GroupBy groupBy = new GroupBy(pipes.getLast(), field);
        pipes.add(groupBy);
        setPipes(pipes);

        return new LocalGroupByDataStream(this);
    }

    @Override
    public void writeTo(URI location, String delimitor) {
        Scheme scheme = new TextDelimited(true, delimitor);
        Tap sinkTap = new FileTap(scheme, location.getPath());
        LinkedList<Pipe> pipes = getPipes();

        setFlowDef(getFlowDef().addSource(getSourcePipe(), getStreamSource().getSourceTap())
                .addTailSink(pipes.getLast(), sinkTap).setName(name));
        assert getFlowDef() != null;
        Flow flow = new LocalFlowConnector().connect(getFlowDef());
        flow.complete();
    }
}
