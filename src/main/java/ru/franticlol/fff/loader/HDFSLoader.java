package ru.franticlol.fff.loader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HDFSLoader<T> implements Loader<T>{
    @Override
    public void load(List<T> objects) throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("/home/nikita/Apache/hadoop/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/home/nikita/Apache/hadoop/etc/hadoop/hdfs-site.xml"));
        FileSystem fileSystem = FileSystem.get(configuration);

        String fileName = "mongo_" + Thread.currentThread().getName() + ".json";
        Path hdfsWritePath = new Path("/user/" + fileName);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        for(T document : objects) {
            JsonElement je = jp.parse((String) document);
            String prettyJsonString = gson.toJson(je);
            prettyJsonString.replace("\n", "\r\n");
            bufferedWriter.write(prettyJsonString);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
        fileSystem.close();
        //переводить в зукипере портции в отдельную папку завершенных задач
    }
}
