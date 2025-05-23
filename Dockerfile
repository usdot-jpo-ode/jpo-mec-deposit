FROM maven:3.8-eclipse-temurin-21-alpine AS builder

WORKDIR /home

# Copy the main project files
COPY ./jpo-mec-deposit/pom.xml ./jpo-mec-deposit/
COPY ./jpo-mec-deposit/lib ./jpo-mec-deposit/lib
COPY ./jpo-mec-deposit/checkstyle.xml ./jpo-mec-deposit/checkstyle.xml
COPY ./jpo-mec-deposit/src ./jpo-mec-deposit/src

RUN cd jpo-mec-deposit && mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /home

COPY --from=builder /home/jpo-mec-deposit/src/main/resources/application.yaml /home
# Use wildcard to match the JAR file regardless of version
COPY --from=builder /home/jpo-mec-deposit/target/jpo-mec-deposit.jar /home/jpo-mec-deposit.jar

ENTRYPOINT ["java", \
	"-jar", \
	"/home/jpo-mec-deposit.jar"]
