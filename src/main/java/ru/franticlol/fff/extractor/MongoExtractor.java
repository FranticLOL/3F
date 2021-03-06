package ru.franticlol.fff.extractor;

import com.mongodb.client.*;
import org.bson.Document;
import ru.franticlol.fff.commons.ZookeeperConf;

import java.util.ArrayList;
import java.util.List;


public class MongoExtractor<T> implements Extractor<T>{
    ZookeeperConf configuration;

    public MongoExtractor(ZookeeperConf configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<T> extract() {
        System.out.println("Extracting...");
        List<String> documents = new ArrayList<>();
        Long threadCount = Long.valueOf(configuration.getData("/conf/threadCount"));
        MongoClient mongoClient = MongoClients.create(configuration.getData("/conf/mongo"));
        MongoDatabase database = mongoClient.getDatabase(configuration.getData("/conf/dbName"));
        MongoCollection<Document> collection = database.getCollection(configuration.getData("/conf/collectionName"));

        Long limitCount = (collection.countDocuments() / threadCount) + 1;
        Long skipCount = configuration.findFirst("/conf/freePartition");

        for(Document document : collection.find().skip(Math.toIntExact(skipCount)).limit(Math.toIntExact(limitCount))) {
            documents.add(document.toJson());
        }
        return (List<T>) documents;
    }
}
