package ru.franticlol.fff.transformer;

import java.util.Map;

public interface Transformer<K, T> {
    Map<K, T> process(Map<K, T> objects);
}
