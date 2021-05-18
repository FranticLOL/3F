package ru.franticlol.fff.transformer;

import ru.franticlol.fff.commons.ZookeeperConf;

import java.util.Map;

public class AbstractTransformer<K, T> implements Transformer<K, T> {
    ZookeeperConf configuration;

    public AbstractTransformer(ZookeeperConf configuration) {
        this.configuration = configuration;
    }

    @Override
    public Map<K, T> process(Map<K, T> objects) {
        return objects;
    }
}
