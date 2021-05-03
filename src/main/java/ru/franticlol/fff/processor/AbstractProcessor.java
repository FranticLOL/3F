package ru.franticlol.fff.processor;

import ru.franticlol.fff.commons.ZookeeperConf;

import java.util.Map;

public class AbstractProcessor<K, T> implements Processor<K, T> {
    ZookeeperConf configuration;

    public AbstractProcessor(ZookeeperConf configuration) {
        this.configuration = configuration;
    }

    @Override
    public Map<K, T> process(Map<K, T> objects) {
        return objects;
    }
}
