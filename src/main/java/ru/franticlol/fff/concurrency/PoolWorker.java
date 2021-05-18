package ru.franticlol.fff.concurrency;

public class PoolWorker<T extends Runnable> extends Thread {
    private ResourcePool<T> resourcePool;

    PoolWorker(ResourcePool<T> resourcePool){
        this.resourcePool = resourcePool;
    }

    @Override
    public void run() {
        while(!resourcePool.tasksIsEmpty()) {
            T task = resourcePool.pollTask();
            System.out.println(this.getName() + ": got from queue : " + task.toString());
            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
