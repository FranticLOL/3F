package ru.franticlol.fff.concurrency;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ResourcePool<T extends Runnable> {
    private int poolSize;
    private Queue<T> tasks;
    private List<PoolWorker<T>> resources;

    public ResourcePool(List<T> tasks, int poolSize) {
        this.tasks = new LinkedList<>();
        this.resources = new ArrayList<>();
        this.tasks.addAll(tasks);
        this.poolSize = poolSize;
        for(int i = 0; i < poolSize; ++i) {
            PoolWorker<T> worker = new PoolWorker<>(this);
            resources.add(worker);
        }
    }

    public synchronized T pollResource() {
        return tasks.poll();
    }

    public synchronized boolean tasksIsEmpty() {
        return tasks.isEmpty();
    }

    public synchronized void addTask(T task){
        tasks.offer(task);
    }

    public void start() {
        resources.forEach(PoolWorker::start);
        resources.forEach(PoolWorker::run);
    }

    public void run() {
        resources.forEach(PoolWorker::run);
    }

    public void stop() {
        resources.forEach(PoolWorker::interrupt);
    }
}