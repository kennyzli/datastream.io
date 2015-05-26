package org.tusk.stream;

import cascading.tap.Tap;

public interface StreamSource {
    Tap getSourceTap();
}
