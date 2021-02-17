package ru.franticlol.fff.commons;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import ru.franticlol.fff.commons.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZookeeperConf {
    Configuration configuration;

    public ZookeeperConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public void startZookeeperConfiguration() {
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

            if (zooKeeper.exists("/conf/freePartition", false) == null) {
                zooKeeper.create("/conf/freePartition", null, acls, CreateMode.PERSISTENT);
            }

            if (zooKeeper.exists("/conf/partitionInWork", false) == null) {
                zooKeeper.create("/conf/partitionInWork", null, acls, CreateMode.PERSISTENT);
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteZookeeperConfiguration(String key) {
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

            String znodePath = "/" + key;
            if (zooKeeper.exists("/conf" + znodePath, false) != null) {
                zooKeeper.delete("/conf" + znodePath, zooKeeper.exists("/conf" + znodePath, true).getVersion());
            }

        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }
    }

    public void setZookeeperConfiguration(String key, byte[] data) {
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

            String znodePath = "/" + key;
            if (zooKeeper.exists("/conf" + znodePath, false) != null) {
                zooKeeper.setData("/conf" + znodePath, data, zooKeeper.exists("/conf" + znodePath, true).getVersion());
            }

        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }
    }

    public void addZookeeperConfiguration(String key, byte[] data) {
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

            String znodePath = "/" + key;
            if (zooKeeper.exists("/conf" + znodePath, false) == null) {
                zooKeeper.create("/conf" + znodePath, data, acls, CreateMode.PERSISTENT);
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | KeeperException | IOException e) {
            e.printStackTrace();
        }

    }

    public synchronized Long findFirst(String key) {
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

            List<String> freePartitionsList = zooKeeper.getChildren(key, null);
            Long freePartition = Long.valueOf(getData("/conf/freePartition/" + freePartitionsList.get(0)));
            moveConfiguration("freePartition/" + freePartitionsList.get(0), "partitionInWork/" + freePartitionsList.get(0));
            return freePartition;
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | IOException | KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void moveConfiguration(String oldKey, String newKey) {
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

            List<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;

            int sessionTimeout = 2000;
            ZooKeeper zooKeeper = null;
            synchronized (lock) {
                zooKeeper = new ZooKeeper(server, sessionTimeout, connectionWatcher);
                lock.wait();
            }

            String znodeOldPath = "/" + oldKey;
            String znodeNewPath = "/" + newKey;
            if (zooKeeper.exists("/conf" + znodeNewPath, false) == null) {
                zooKeeper.create("/conf" + znodeNewPath, getData("/conf" + znodeOldPath).getBytes(StandardCharsets.UTF_8), acls, CreateMode.PERSISTENT);
            } else {
                zooKeeper.setData("/conf" + znodeNewPath, getData("/conf" + znodeOldPath).getBytes(StandardCharsets.UTF_8), zooKeeper.exists("/conf" + znodeNewPath, true).getVersion());
            }

            zooKeeper.delete("/conf" + znodeOldPath, zooKeeper.exists("/conf" + znodeOldPath, true).getVersion());

        } catch (
                FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | KeeperException |
                IOException e) {
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
            return new String(data);
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (InterruptedException | IOException | KeeperException e) {
            e.printStackTrace();
        }

        return null;
    }
}
