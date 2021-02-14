package ru.franticlol.fff.commons;

public class CommandLineParser {

    //добавить обработку ошибок
    public static CommandLine parse(String[] args) {
        Options options = new Options();
        for(int i = 0; i < args.length; ++i) {
            if(args[i].equals("-f")) {
                options.addOption(new Option("f", args[i+1], true));
            }
        }
        return new CommandLine(options);
    }
}
