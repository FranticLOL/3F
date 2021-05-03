FROM openjdk:11
COPY 3F-1.0-all.jar /
COPY config.txt /config/
COPY hdfs-site.xml /home/nikita/Apache/hadoop/etc/hadoop/hdfs-site.xml
COPY core-site.xml /home/nikita/Apache/hadoop/etc/hadoop/core-site.xml
CMD ["java", "-jar", "/3F-1.0-all.jar", "-f", "/config/config.txt"]
