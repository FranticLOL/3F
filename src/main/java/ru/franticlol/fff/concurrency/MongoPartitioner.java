package ru.franticlol.fff.concurrency;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import ru.franticlol.fff.commons.ZookeeperConf;

import java.nio.charset.StandardCharsets;

public class MongoPartitioner implements Partitioner {
    ZookeeperConf zookeeperConf;

    public MongoPartitioner(ZookeeperConf zookeeperConf) {
        this.zookeeperConf = zookeeperConf;
    }

    public void partition() {
        Long threadCount = Long.valueOf(zookeeperConf.getData("/conf/threadCount"));
        MongoClient mongoClient = MongoClients.create(zookeeperConf.getData("/conf/mongo"));
        MongoDatabase database = mongoClient.getDatabase(zookeeperConf.getData("/conf/dbName"));
        MongoCollection<Document> collection = database.getCollection(zookeeperConf.getData("/conf/collectionName"));

        Long docCount = collection.countDocuments();
        for(long i = 0; i < threadCount; ++i) {
            zookeeperConf.addZookeeperConfiguration("freePartition/" + i , String.valueOf(((i * ((docCount / threadCount) + 1)))).getBytes(StandardCharsets.UTF_8));
        }
    }
}
