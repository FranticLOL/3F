package ru.franticlol.fff.commons;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ConfigurationParser {

    public static Map<String, String> parse(File configFile) throws FileNotFoundException {
        Map<String, String> configMap = new HashMap<>();
        Scanner scanner = new Scanner(configFile);
        scanner.useDelimiter("\n");
        while(scanner.hasNext()) {
            String line = scanner.next();
            if(line != null && !line.equals("")) {
                String key = line.split(" = ")[0];
                String value = line.split(" = ")[1];
                configMap.put(key, value);
            }
        }

        return configMap;
    }
}
