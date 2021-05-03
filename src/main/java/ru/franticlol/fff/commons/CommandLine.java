package ru.franticlol.fff.commons;

import java.util.HashMap;
import java.util.Map;

public class CommandLine {
    Map<String, Option> optionMap;

    CommandLine(Options options) {
        optionMap = new HashMap<>();
        for (Option option : options.getOptions()) {
            optionMap.put(option.getId(), option);
        }
    }

    public Option getOption(String name) {
        return optionMap.get(name);
    }
}
