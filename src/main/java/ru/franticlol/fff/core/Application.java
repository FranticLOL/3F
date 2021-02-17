package ru.franticlol.fff.core;

import ru.franticlol.fff.commons.*;
import ru.franticlol.fff.concurrency.Partitioner;
import ru.franticlol.fff.concurrency.ResourcePool;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

//добавить логи
public class Application {

    public static void main(String[] args) {
        CommandLine commandLine = CommandLineParser.parse(args);
        File configFile = new File(commandLine.getOption("f").getOptionName());
        try {
            Configuration configuration = new Configuration(ConfigurationParser.parse(configFile));
            ZookeeperConf zookeeperConf = new ZookeeperConf(configuration);
            zookeeperConf.startZookeeperConfiguration();

            Class<?> partitionClass = Class.forName(zookeeperConf.getData("/conf/partitioner"));
            Constructor<?> partitionClassConstructor = partitionClass.getConstructor(ZookeeperConf.class);
            Partitioner partitioner = (Partitioner) partitionClassConstructor.newInstance(zookeeperConf);

            partitioner.partition();

            List<BatchProcess> tasksList = new ArrayList<>();


            for (int i = 0; i < Integer.parseInt(zookeeperConf.getData("/conf/threadCount")); ++i) {
                tasksList.add(new BatchProcess(zookeeperConf));
            }

            ResourcePool<BatchProcess> pool = new ResourcePool<>(tasksList, Integer.parseInt(zookeeperConf.getData("/conf/threadCount")));

            while (true) {
                pool.run();
                if (pool.tasksIsEmpty()) {
                    Thread.sleep(3000);
                    if(pool.tasksIsEmpty()) {
                        System.out.println("Poll has stopped due to no new tasks.");
                        return;
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | InstantiationException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e){ //| InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
