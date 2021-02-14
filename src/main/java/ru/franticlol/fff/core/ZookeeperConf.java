package ru.franticlol.fff.core;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import ru.franticlol.fff.commons.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZookeeperConf {
    Configuration configuration;

    ZookeeperConf(Configuration configuration) {
        this.configuration = configuration;
    }

    void setZookeeperConfiguration() {
        try {
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
            ZooKeeper zooKeeper = null;
            synchronized (lock) {
                zooKeeper = new ZooKeeper(server, sessionTimeout, connectionWatcher);
                lock.wait();
            }

            List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
            if (zooKeeper.exists("/conf", false) == null) {
                zooKeeper.create("/conf", null, acls, CreateMode.PERSISTENT);
            }

            for (String key : configuration.getConfigurationMap().keySet()) {
                String znodePath = "/" + key;
                if (zooKeeper.exists("/conf" + znodePath, false) == null) {
                    zooKeeper.create("/conf" + znodePath, configuration.getConfigurationMap().get(key).getBytes(StandardCharsets.UTF_8), acls, CreateMode.PERSISTENT);
                } else {
                    zooKeeper.setData("/conf" + znodePath, configuration.getConfigurationMap().get(key).getBytes(StandardCharsets.UTF_8), zooKeeper.exists("/conf" + znodePath, true).getVersion());
                }
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    public String getData(String nodePath) {
        try {
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
            ZooKeeper zooKeeper = null;
            synchronized (lock) {
                zooKeeper = new ZooKeeper(server, sessionTimeout, connectionWatcher);
                lock.wait();
            }

            byte[] data = zooKeeper.getData(nodePath, null, null);
            System.out.println("Result: " + new String(data, "UTF-8"));
            return new String(data);
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }
}
