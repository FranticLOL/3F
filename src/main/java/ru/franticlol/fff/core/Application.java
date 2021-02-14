package ru.franticlol.fff.core;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import ru.franticlol.fff.commons.CommandLine;
import ru.franticlol.fff.commons.CommandLineParser;
import ru.franticlol.fff.commons.Configuration;
import ru.franticlol.fff.commons.ConfigurationParser;
import ru.franticlol.fff.extractor.Extractor;
import ru.franticlol.fff.extractor.MongoExtractor;
import ru.franticlol.fff.loader.HDFSLoader;
import ru.franticlol.fff.loader.Loader;
import ru.franticlol.fff.processor.MongoProcessor;
import ru.franticlol.fff.processor.Processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

//добавить логи
public class Application {
    public static void main(String[] args) {
        CommandLine commandLine = CommandLineParser.parse(args);
        File configFile = new File(commandLine.getOption("f").getOptionName());
        try {
            Configuration configuration = new Configuration(ConfigurationParser.parse(configFile));
            ZookeeperConf zookeeperConf = new ZookeeperConf(configuration);
            zookeeperConf.setZookeeperConfiguration();

            Extractor extractor = new MongoExtractor(zookeeperConf);
            Processor processor = new MongoProcessor(configuration);
            List<Object> objects = extractor.extract();
            Loader loader = new HDFSLoader();
            loader.load(objects);
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
