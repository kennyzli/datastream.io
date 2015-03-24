package org.datastream.stream.impl.hadoop;

import java.net.URI;

import org.datastream.stream.StreamSource;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

public class HadoopStreamSource implements StreamSource {

    private URI location;

    /**
     * private StreamSource
     * 
     */
    public HadoopStreamSource(URI location) {
        this.location = location;
    }

    public URI getLocation() {
        return location;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Tap getSourceTap() {
        Scheme scheme = new TextDelimited(true, ",");

        Tap tap = new FileTap(scheme, location.getPath());
        return tap;
    }

}
