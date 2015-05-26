package io.tusk.stream.impl.local;

import io.tusk.stream.GroupByDataStream;
import io.tusk.stream.StreamData;
import io.tusk.stream.StreamSource;
import io.tusk.stream.impl.AbstractDataStreamImpl;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
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

    public LocalDataStream(LocalDataStream stream) {
        super(stream);
    }

    public LocalDataStream(String name, LocalStreamSource dataSource) {
        assert dataSource != null;
        this.name = name;

        this.source = dataSource;
        TapPipe tapSource = new TapPipe();
        tapSource.sourcePipe = new Pipe(name + ":source");
        tapSource.sourceTap = dataSource.getSourceTap();

        // tap pipes combination
        List<TapPipe> sources = new ArrayList<AbstractDataStreamImpl.TapPipe>();
        sources.add(tapSource);
        setSourcePipe(sources);

        // pipes
        LinkedList<Pipe> list = getPipes();
        list.add(tapSource.sourcePipe);
        setPipes(list);

    }

    @Override
    public StreamSource getStreamSource() {
        return source;
    }

    @Override
    protected void setStreamSource(StreamSource source) {
        this.source = (LocalStreamSource) source;
    }

    @Override
    public GroupByDataStream<StreamData> groupBy(String... fields) {
        Fields field = new Fields();
        for (String fieldName : fields) {
            field = field.append(new Fields(fieldName));
        }
        LinkedList<Pipe> pipes = getPipes();
        assert pipes.size() > 0;
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
        FlowDef def = getFlowDef();

        for (TapPipe tPipe : getSourcePipe()) {
            def = def.addSource(tPipe.sourcePipe, tPipe.sourceTap);
        }

        setFlowDef(def.addTailSink(pipes.getLast(), sinkTap).setName(name));

        assert getFlowDef() != null;
        Flow flow = new LocalFlowConnector().connect(getFlowDef());
        flow.complete();
    }
}
