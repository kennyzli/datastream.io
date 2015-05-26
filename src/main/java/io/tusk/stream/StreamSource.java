package io.tusk.stream;

import cascading.tap.Tap;

public interface StreamSource {
    Tap getSourceTap();
}
