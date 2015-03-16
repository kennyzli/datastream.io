package org.datastream.stream.impl;

import java.net.URI;

import org.datastream.stream.StreamSource;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

public class LocalCSVStreamSource implements StreamSource {

    private URI location;

    /**
     * private CSVStreamSource
     * 
     */
    protected LocalCSVStreamSource(URI location) {
        this.location = location;
    }

    public URI getLocation() {
        return location;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    Tap getSourceTap() {
        Scheme scheme = new TextDelimited(true, ",");

        Tap tap = new FileTap(scheme, location.getPath());
        return tap;
    }

}
