FROM openjdk:11
COPY ../build/libs/3F-1.0-all.jar /
COPY config.txt /config/
CMD ["java", "-jar", "/3F-1.0-all.jar", "-f", "/config/config.txt"]
