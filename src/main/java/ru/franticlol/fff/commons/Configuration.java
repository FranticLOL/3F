package ru.franticlol.fff.commons;

import java.util.Map;

public class Configuration {
    Map<String, String> configurationMap;

    public Configuration(Map<String, String> map) {
        configurationMap = map;
    }

    public Map<String, String> getConfigurationMap() {
        return configurationMap;
    }
}
