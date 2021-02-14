package ru.franticlol.fff.extractor;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import ru.franticlol.fff.commons.Configuration;
import ru.franticlol.fff.core.ZookeeperConf;

import java.util.ArrayList;
import java.util.List;


public class MongoExtractor<Object> implements Extractor<Object>{
    ZookeeperConf configuration;

    public MongoExtractor(ZookeeperConf configuration) {
        this.configuration = configuration;
    }

    @Override
    public List<Object> extract() {
        List<String> documents = new ArrayList<>();
        //реализовать получение данных и передачу их в процессор, а оттуда в лоадер
        MongoClient mongoClient = MongoClients.create(configuration.getData("/conf/mongo"));
        MongoDatabase database = mongoClient.getDatabase(configuration.getData("/conf/dbName"));
        MongoCollection<Document> collection = database.getCollection(configuration.getData("/conf/collectionName"));
        for(Document document : collection.find()) {
            documents.add(document.toJson());
        }
        return (List<Object>) documents;
    }
}
