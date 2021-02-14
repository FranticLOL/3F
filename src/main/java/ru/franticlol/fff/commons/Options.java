package ru.franticlol.fff.commons;

import java.util.ArrayList;
import java.util.List;

public class Options {
    private List<Option> options;

    Options() {
        this.options = new ArrayList<>();
    }

    Options(List<Option> options) {
        this.options = options;
    }

    List<Option> getOptions(){
        return options;
    }


    void addOption(Option option) {
        options.add(option);
    }
}
