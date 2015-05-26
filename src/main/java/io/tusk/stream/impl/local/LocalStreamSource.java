package io.tusk.stream.impl.local;

import io.tusk.stream.StreamSource;

import java.net.URI;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

public class LocalStreamSource implements StreamSource {

    private URI location;

    /**
     * private StreamSource
     * 
     */
    public LocalStreamSource(URI location) {
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
