package ru.franticlol.fff.commons;

public class Option {
    private String id;
    private String optionName;
    private Boolean required;

    Option(String id, String optionName, Boolean required) {
        this.id = id;
        this.optionName = optionName;
        this.required = required;
    }

    public String getId() {
         return id;
    }

    public String getOptionName() {
        return optionName;
    }

    public Boolean optionIsRequired() {
        return required;
    }
}
