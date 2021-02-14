package ru.franticlol.fff.loader;

import java.io.IOException;
import java.util.List;

public interface Loader<T> {
    void load(List<T> objects) throws IOException;
}
