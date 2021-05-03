package ru.franticlol.fff.loader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ru.franticlol.fff.commons.ZookeeperConf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HDFSLoader<K, T> implements Loader<K, T> {
    ZookeeperConf zookeeperConf;

    public HDFSLoader(ZookeeperConf zookeeperConf) {
        this.zookeeperConf = zookeeperConf;
    }

    @Override
    public void load(Map<K, T> objects) throws IOException {
        if(objects.isEmpty()) {
            return;
        }

        System.out.println("Loading started");

        try {
            Thread.sleep(30000);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }

        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        configuration.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        configuration.addResource(new Path("/home/nikita/Apache/hadoop/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/home/nikita/Apache/hadoop/etc/hadoop/hdfs-site.xml"));

        FileSystem fileSystem = FileSystem.get(configuration);

        String fileName = InetAddress.getLocalHost().getCanonicalHostName() + "_" + Thread.currentThread().getName() + "_" + objects.keySet().stream().findFirst().get() + ".json";
        Path hdfsWritePath = new Path("/user/" + fileName);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        try {
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            for (T documentList : objects.values()) {
                for(String document : (List<String>) documentList) {
                    JsonElement je = jp.parse(document);
                    String prettyJsonString = gson.toJson(je);
                    bufferedWriter.write(prettyJsonString);
                    bufferedWriter.newLine();
                }
            }
            System.out.println("Loading finished");
            bufferedWriter.close();
            fileSystem.close();

            if (objects.keySet().stream().findFirst().isPresent()) {
                String taskKey = (String) objects.keySet().stream().findFirst().get();
                zookeeperConf.setEndTask(taskKey);
            }

        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
