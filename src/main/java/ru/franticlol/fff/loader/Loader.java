package ru.franticlol.fff.loader;

import java.io.IOException;
import java.util.Map;

public interface Loader<K, T> {
    void load(Map<K, T> objects) throws IOException;
}
