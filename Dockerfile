FROM maven:3.8-eclipse-temurin-21-alpine AS builder

WORKDIR /home

COPY ./jpo-mec-deposit/pom.xml ./jpo-mec-deposit/
COPY ./jpo-mec-deposit/lib ./jpo-mec-deposit/lib
COPY ./jpo-mec-deposit/checkstyle.xml ./jpo-mec-deposit/checkstyle.xml

# Download dependencies alone to cache them first
WORKDIR /home/jpo-mec-deposit
RUN mvn dependency:resolve

# Copy the source code and build the project
COPY ./jpo-mec-deposit/src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /home

COPY --from=builder /home/jpo-mec-deposit/src/main/resources/application.yaml /home
COPY --from=builder /home/jpo-mec-deposit/target/jpo-mec-deposit.jar /home

ENTRYPOINT ["java", \
	"-jar", \
	"/home/jpo-mec-deposit.jar"]
