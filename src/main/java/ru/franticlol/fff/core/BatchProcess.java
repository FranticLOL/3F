package ru.franticlol.fff.core;

import ru.franticlol.fff.commons.Configuration;
import ru.franticlol.fff.commons.ConfigurationParser;
import ru.franticlol.fff.commons.ZookeeperConf;
import ru.franticlol.fff.extractor.Extractor;
import ru.franticlol.fff.loader.Loader;
import ru.franticlol.fff.processor.Processor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class BatchProcess implements Runnable{
    ZookeeperConf zookeeperConf;

    BatchProcess(ZookeeperConf zookeeperConf) {
        this.zookeeperConf = zookeeperConf;
    }
    @Override
    public void run() {
        try {
            Class<?> extractorClass = Class.forName(zookeeperConf.getData("/conf/extractor"));
            Constructor<?> extractorClassConstructor = extractorClass.getConstructor(ZookeeperConf.class);
            Extractor extractor = (Extractor) extractorClassConstructor.newInstance(new Object[] { zookeeperConf });

            Class<?> processorClass = Class.forName(zookeeperConf.getData("/conf/processor"));
            Constructor<?> processorClassConstructor = processorClass.getConstructor(ZookeeperConf.class);
            Processor processor = (Processor) processorClassConstructor.newInstance(new Object[] { zookeeperConf });

            Class<?> loaderClass = Class.forName(zookeeperConf.getData("/conf/loader"));
            Constructor<?> loaderClassConstructor = loaderClass.getConstructor();
            Loader loader = (Loader) loaderClassConstructor.newInstance(new Object[] {});

            List<String> objects = extractor.extract();
            loader.load(objects);
            System.out.println(Thread.currentThread().getName() + " has done a job.");
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException | InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
