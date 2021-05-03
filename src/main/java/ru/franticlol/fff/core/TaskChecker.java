package ru.franticlol.fff.core;

import ru.franticlol.fff.commons.ZookeeperConf;

public class TaskChecker {

    public static boolean checkTaskStartedFlag(ZookeeperConf zookeeperConf) {
        if(zookeeperConf.getData("/conf/workStartedFlag") != null) {
            return String.valueOf(zookeeperConf.getData("/conf/workStartedFlag").toCharArray()).equals("true");
        } else {
            return false;
        }
    }

    public static boolean isFreeTasksEmpty(ZookeeperConf zookeeperConf) {
        return zookeeperConf.isFreeTasksEmpty();
    }

    public static boolean checkWorkIsOver(ZookeeperConf zookeeperConf) {
        return zookeeperConf.isTasksInWorkEmpty();
    }
}
