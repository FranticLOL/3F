package ru.franticlol.fff.extractor;

import com.mongodb.client.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.bson.Document;
import ru.franticlol.fff.commons.ZookeeperConf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MongoExtractor<K, T> implements Extractor<K, T> {
    ZookeeperConf zookeeperConf;

    public MongoExtractor(ZookeeperConf zookeeperConf) {
        this.zookeeperConf = zookeeperConf;
    }

    @Override
    public Map<K, T> extract() {
        System.out.println("Extracting...");
        Map<String, List<String>> documents = new HashMap<>();
        List<String> documentsList = new ArrayList<>();
        Long batchSize = Long.valueOf(zookeeperConf.getData("/conf/batchSize"));
        MongoClient mongoClient = MongoClients.create(zookeeperConf.getData("/conf/mongo"));
        MongoDatabase database = mongoClient.getDatabase(zookeeperConf.getData("/conf/dbName"));
        MongoCollection<Document> collection = database.getCollection(zookeeperConf.getData("/conf/collectionName"));

        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConf.getConfiguration().getConfigurationMap().get("zookeeper"), new ExponentialBackoffRetry(1000, 3));
        client.start();
        InterProcessSemaphoreMutex sharedLock = new InterProcessSemaphoreMutex(
                client, "/conf");

        Long skipCount = null;
        try {
            sharedLock.acquire(); //startTask = getFreePartition + setPartitionToWorkStatus
            skipCount = zookeeperConf.findFirstFreeTask();
            System.out.println("SkipCount is " + skipCount);
            sharedLock.release();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if (skipCount != null) {
            if(skipCount != -1L) {
                for (Document document : collection.find().skip(Math.toIntExact(skipCount)).limit(Math.toIntExact(batchSize))) {
                    documentsList.add(document.toJson());
                }
                documents.put(Long.toString(skipCount / batchSize), documentsList);
            }
        } else {
            System.out.println("SkipCount is null. Check MongoExtractor");
        }

        return (Map<K, T>) documents;
    }
}
