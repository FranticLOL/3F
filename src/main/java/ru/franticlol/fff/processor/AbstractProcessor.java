package ru.franticlol.fff.processor;

import ru.franticlol.fff.commons.ZookeeperConf;

import java.util.List;

public class AbstractProcessor<T> implements Processor<T> {
    ZookeeperConf configuration;

    public AbstractProcessor(ZookeeperConf configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<T> process(List<T> objects) {
        return objects;
    }
}
