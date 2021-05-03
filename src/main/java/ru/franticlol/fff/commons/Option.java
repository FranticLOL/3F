package ru.franticlol.fff.commons;

public class Option {
    private String id;
    private String optionValue;
    private Boolean required;

    Option(String id, String optionValue, Boolean required) {
        this.id = id;
        this.optionValue = optionValue;
        this.required = required;
    }

    public String getId() {
         return id;
    }

    public String getOptionValue() {
        return optionValue;
    }

    public Boolean optionIsRequired() {
        return required;
    }
}
