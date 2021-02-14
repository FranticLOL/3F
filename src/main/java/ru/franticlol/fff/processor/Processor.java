package ru.franticlol.fff.processor;

import java.util.List;

public interface Processor<T> {
    List<T> process(List<T> objects);
}
