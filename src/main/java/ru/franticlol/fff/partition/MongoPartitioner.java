package ru.franticlol.fff.partition;

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
        Long batchSize = Long.valueOf(zookeeperConf.getData("/conf/batchSize"));
        System.out.println("Connecting to MongoDB");
        MongoClient mongoClient = MongoClients.create(zookeeperConf.getData("/conf/mongo"));
        MongoDatabase database = mongoClient.getDatabase(zookeeperConf.getData("/conf/dbName"));
        MongoCollection<Document> collection = database.getCollection(zookeeperConf.getData("/conf/collectionName"));
        Long docCount = collection.countDocuments();
        Long partitionCount = docCount % batchSize == 0 ? docCount / batchSize : (docCount / batchSize) + 1;
        for(long i = 0; i < partitionCount; ++i) {
            System.out.println(i * batchSize);
            zookeeperConf.addZookeeperConfiguration("freePartition/" + i , String.valueOf(((i * batchSize))).getBytes(StandardCharsets.UTF_8));
        }
    }
}
