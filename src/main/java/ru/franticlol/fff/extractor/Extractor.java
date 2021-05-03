package ru.franticlol.fff.extractor;

import java.util.Map;

public interface Extractor<K, T> {
    Map<K, T> extract();
}
