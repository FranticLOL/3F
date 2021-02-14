package ru.franticlol.fff.processor;

import ru.franticlol.fff.commons.Configuration;

import java.util.List;

public class MongoProcessor<T> implements Processor<T> {
    Configuration configuration;

    public MongoProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<T> process(List<T> objects) {
        return objects;
    }
}
