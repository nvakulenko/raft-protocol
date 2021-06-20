FROM maven:3.6.3-jdk-11 AS build
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN mvn -f /usr/src/app/pom.xml clean package

FROM openjdk:11
COPY --from=build /usr/src/app/target/raft-protocol-1.0-SNAPSHOT.jar /usr/app/raft-protocol-1.0-SNAPSHOT.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/usr/app/raft-protocol-1.0-SNAPSHOT.jar"]
