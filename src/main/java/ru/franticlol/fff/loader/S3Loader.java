package ru.franticlol.fff.loader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonParser;
import ru.franticlol.fff.commons.ZookeeperConf;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3Loader<K, T> implements Loader<K, T> {
    ZookeeperConf zookeeperConf;

    public S3Loader(ZookeeperConf zookeeperConf) {
        this.zookeeperConf = zookeeperConf;
    }

    @Override
    public void load(Map<K, T> objects) throws IOException {
        if(objects.isEmpty()) {
            return;
        }

        System.out.println("Loading started");

        String fileName = InetAddress.getLocalHost().getCanonicalHostName() + "_" + Thread.currentThread().getName() + "_" + objects.keySet().stream().findFirst().get() + ".json";

  //      Gson gson = new GsonBuilder().setPrettyPrinting().create();
  //      JsonParser jp = new JsonParser();

        FileOutputStream out = new FileOutputStream("/" + fileName);

        for (T documentList : objects.values()) {
            for(String document : (List<String>) documentList) {
//                JsonElement je = jp.parse(document);
//                String prettyJsonString = gson.toJson(je);
                out.write(document.getBytes(StandardCharsets.UTF_8));
                out.write("\n".getBytes(StandardCharsets.UTF_8));
            }
        }
        System.out.println("Loading finished");

        out.close();

        AWSCredentials credentials = new BasicAWSCredentials(
                "AKIA3G4GSWO6CZFGSFAQ",
                "1MsT9vOlIWmJZ1E1x1hRsSQzD83U1DYPFxfBAsoo"
        );

        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.EU_CENTRAL_1)
                .build();

        String bucketName = "users-3f";

        s3client.putObject(
                bucketName,
                "/" + fileName,
                new File("/" + fileName)
        );





        if (objects.keySet().stream().findFirst().isPresent()) {
            String taskKey = (String) objects.keySet().stream().findFirst().get();
            zookeeperConf.setEndTask(taskKey);
        }
        /*FileSystem fileSystem = FileSystem.get(configuration);

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
        }*/
    }
}