package ru.franticlol.fff.processor;

import java.util.Map;

public interface Processor<K, T> {
    Map<K, T> process(Map<K, T> objects);
}
