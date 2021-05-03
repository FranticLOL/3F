package ru.franticlol.fff.core;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import ru.franticlol.fff.commons.*;
import ru.franticlol.fff.concurrency.ResourcePool;
import ru.franticlol.fff.partition.Partitioner;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class Application {

    public static void main(String[] args) {
        System.out.println("Starting application...");
        CommandLine commandLine = CommandLineParser.parse(args);
        File configFile = new File(commandLine.getOption("f").getOptionValue());
        try {
            Configuration configuration = new Configuration(ConfigurationParser.parse(configFile));
            ZookeeperConf zookeeperConf = new ZookeeperConf(configuration);

            CuratorFramework client = CuratorFrameworkFactory.newClient(configuration.getConfigurationMap().get("zookeeper"), new ExponentialBackoffRetry(1000, 3));
            client.start();
            InterProcessSemaphoreMutex sharedLock = new InterProcessSemaphoreMutex(
                    client, "/conf");

            sharedLock.acquire();
            if (!TaskChecker.checkTaskStartedFlag(zookeeperConf)) {
                System.out.println(TaskChecker.checkTaskStartedFlag(zookeeperConf));
                System.out.println("Starting configuration...");
                zookeeperConf.startZookeeperConfiguration();
                System.out.println("Configuration ended");

                System.out.println("Start partitioning");
                Class<?> partitionClass = Class.forName(zookeeperConf.getData("/conf/partitioner"));
                Constructor<?> partitionClassConstructor = partitionClass.getConstructor(ZookeeperConf.class);
                Partitioner partitioner = (Partitioner) partitionClassConstructor.newInstance(zookeeperConf);

                partitioner.partition();
                zookeeperConf.setTaskStartedFlag();
                System.out.println(String.valueOf(zookeeperConf.getData("/conf/workStartedFlag").toCharArray()));
                System.out.println("Partitioning ended");
            } else {
                System.out.println("Configuration was ended by another process");
            }
            sharedLock.release();

            List<BatchProcess> tasksList = new ArrayList<>();

            Integer threadCount = Math.toIntExact(Long.parseLong(zookeeperConf.getData("/conf/threadCount")));

            for (int i = 0; i < threadCount; ++i) {
                tasksList.add(new BatchProcess(zookeeperConf));
            }

            ResourcePool<BatchProcess> pool = new ResourcePool<>(tasksList, threadCount);


            System.out.println("Starting pool working...");
            pool.start();
            while (true) {
                if (pool.tasksIsEmpty()) {
                    Thread.sleep(3000);
                    if (!TaskChecker.isFreeTasksEmpty(zookeeperConf)) {
                        for (int i = 0; i < threadCount; ++i) {
                            pool.addTask(new BatchProcess(zookeeperConf));
                        }
                        System.out.println("New task was added to poll");
                        pool.run();
                    }

                    if(TaskChecker.isFreeTasksEmpty(zookeeperConf) && !TaskChecker.checkWorkIsOver(zookeeperConf)) {
                        Thread.sleep(60000);
                        if(TaskChecker.isFreeTasksEmpty(zookeeperConf) && !TaskChecker.checkWorkIsOver(zookeeperConf)) {
                            zookeeperConf.moveAllWorkTasksToFree();
                        }
                    }

                    if (TaskChecker.isFreeTasksEmpty(zookeeperConf) && TaskChecker.checkWorkIsOver(zookeeperConf)) {
                        System.out.println("Poll has stopped due to no new tasks.");
                        pool.stop();
                        zookeeperConf.setTaskEndedFlag();
                        return;
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}