package ru.franticlol.fff.extractor;

import ru.franticlol.fff.commons.Configuration;

import java.util.List;

public interface Extractor<T> {
    List<T> extract();
}
