package ru.franticlol.fff.commons;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZookeeperConf {
    Configuration configuration;
    ZooKeeper zooKeeper;

    public ZookeeperConf(Configuration configuration) {
        this.configuration = configuration;
        String server = configuration.getConfigurationMap().get("zookeeper");
        Object lock = new Object();

        Watcher connectionWatcher = new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper in " + Thread.currentThread().getName());
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        };

        int sessionTimeout = 2000;
        this.zooKeeper = null;

        try {
            synchronized (lock) {
                this.zooKeeper = new ZooKeeper(server, sessionTimeout, connectionWatcher);
                lock.wait();
            }
        } catch (IOException | InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void startZookeeperConfiguration() {
        try {

            List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
            if (zooKeeper.exists("/conf", true) == null) {
                zooKeeper.create("/conf", null, acls, CreateMode.PERSISTENT);
            }

            for (String key : configuration.getConfigurationMap().keySet()) {
                String znodePath = "/" + key;
                if (zooKeeper.exists("/conf" + znodePath, true) == null) {
                    zooKeeper.create("/conf" + znodePath, configuration.getConfigurationMap().get(key).getBytes(StandardCharsets.UTF_8), acls, CreateMode.PERSISTENT);
                } else {
                    zooKeeper.setData("/conf" + znodePath, configuration.getConfigurationMap().get(key).getBytes(StandardCharsets.UTF_8), zooKeeper.exists("/conf" + znodePath, true).getVersion());
                }
            }

            if (zooKeeper.exists("/conf/freePartition", true) == null) {
                zooKeeper.create("/conf/freePartition", null, acls, CreateMode.PERSISTENT);
            }

            if (zooKeeper.exists("/conf/partitionInWork", true) == null) {
                zooKeeper.create("/conf/partitionInWork", null, acls, CreateMode.PERSISTENT);
            }

            if (zooKeeper.exists("/conf/finishedTasks", true) == null) {
                zooKeeper.create("/conf/finishedTasks", null, acls, CreateMode.PERSISTENT);
            }

            if (zooKeeper.exists("/conf/workStartedFlag", true) == null) {
                zooKeeper.create("/conf/workStartedFlag", null, acls, CreateMode.PERSISTENT);
            }

            if (zooKeeper.exists("/conf/workEndedFlag", true) == null) {
                zooKeeper.create("/conf/workEndedFlag", null, acls, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void setZookeeperConfiguration(String key, byte[] data) {
        try {
            String znodePath = "/" + key;
            if (zooKeeper.exists("/conf" + znodePath, false) != null) {
                zooKeeper.setData("/conf" + znodePath, data, zooKeeper.exists("/conf" + znodePath, true).getVersion());
            }

        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void addZookeeperConfiguration(String key, byte[] data) {
        try {
            List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;

            String znodePath = "/" + key;
            if (zooKeeper.exists("/conf" + znodePath, false) == null) {
                zooKeeper.create("/conf" + znodePath, data, acls, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

    }

    public synchronized void setTaskStartedFlag() {
        setZookeeperConfiguration("workStartedFlag", "true".getBytes(StandardCharsets.UTF_8));
        setZookeeperConfiguration("workEndedFlag", "false".getBytes(StandardCharsets.UTF_8));
    }

    public synchronized void setTaskEndedFlag() {
        setZookeeperConfiguration("workStartedFlag", "false".getBytes(StandardCharsets.UTF_8));
        setZookeeperConfiguration("workEndedFlag", "true".getBytes(StandardCharsets.UTF_8));
    }

    public synchronized Long findFirstFreeTask() {
        try {
            List<String> freePartitionsList = zooKeeper.getChildren("/conf/freePartition", null);
            if (freePartitionsList.stream().anyMatch(str -> str.matches("[0-9]+"))) {
                String firstPartition = freePartitionsList.stream().filter(str -> str.matches("[0-9]+")).findFirst().get();
                Long freePartition = Long.valueOf(getData("/conf/freePartition/" + firstPartition));
                moveConfiguration("freePartition/" + firstPartition, "partitionInWork/" + firstPartition);
                return freePartition;
            } else {
                return -1L;
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }

    public boolean isFreeTasksEmpty() {
        try {
            List<String> freePartitionsList = zooKeeper.getChildren("/conf/freePartition", null);
            if (freePartitionsList.stream().anyMatch(str -> str.matches("[0-9]+"))) {
                return false;
            } else {
                return true;
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

        return true;
    }

    public boolean isTasksInWorkEmpty() {
        try {
            List<String> workPartitionsList = zooKeeper.getChildren("/conf/partitionInWork", null);
            if (workPartitionsList.stream().anyMatch(str -> str.matches("[0-9]+"))) {
                return false;
            } else {
                return true;
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

        return true;
    }

    public synchronized void moveAllWorkTasksToFree() {
        try {
            List<String> workPartitionsList = zooKeeper.getChildren("/conf/partitionInWork", null);
            workPartitionsList.stream().filter(str -> str.matches("[0-9]+")).forEach(partition -> moveConfiguration("partitionInWork/" + partition, "freePartition/" + partition));
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void setEndTask(String taskKey) {
        moveConfiguration("partitionInWork/" + taskKey, "finishedTasks/" + taskKey);
    }

    public void moveConfiguration(String oldKey, String newKey) {
        try {
            List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
            String znodeOldPath = "/" + oldKey;
            String znodeNewPath = "/" + newKey;
            if (zooKeeper.exists("/conf" + znodeNewPath, false) == null) {
                zooKeeper.create("/conf" + znodeNewPath, getData("/conf" + znodeOldPath).getBytes(StandardCharsets.UTF_8), acls, CreateMode.PERSISTENT);
            } else {
                zooKeeper.setData("/conf" + znodeNewPath, getData("/conf" + znodeOldPath).getBytes(StandardCharsets.UTF_8), zooKeeper.exists("/conf" + znodeNewPath, true).getVersion());
            }

            zooKeeper.delete("/conf" + znodeOldPath, zooKeeper.exists("/conf" + znodeOldPath, true).getVersion());

        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

    }

    public String getData(String nodePath) {
        try {
            byte[] data = zooKeeper.getData(nodePath, null, null);
            return new String(data);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }
}
