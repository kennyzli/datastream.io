package org.datastream.stream;

import cascading.tap.Tap;

public interface StreamSource {
    Tap getSourceTap();
}
