FROM maven:3.8-eclipse-temurin-21-alpine AS builder

WORKDIR /home

ARG MAVEN_GITHUB_TOKEN
ARG MAVEN_GITHUB_ORG

ENV MAVEN_GITHUB_TOKEN=$MAVEN_GITHUB_TOKEN
ENV MAVEN_GITHUB_ORG=$MAVEN_GITHUB_ORG

COPY ./jpo-mec-deposit/pom.xml ./jpo-mec-deposit/
COPY ./jpo-mec-deposit/checkstyle.xml ./jpo-mec-deposit/checkstyle.xml
COPY ./jpo-mec-deposit/settings.xml ./jpo-mec-deposit/settings.xml

# Download dependencies alone to cache them first
WORKDIR /home/jpo-mec-deposit
RUN mvn -s settings.xml dependency:resolve

# Copy the source code and build the project
COPY ./jpo-mec-deposit/src ./src
RUN mvn -s settings.xml install -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /home

COPY --from=builder /home/jpo-mec-deposit/src/main/resources/application.yaml /home
COPY --from=builder /home/jpo-mec-deposit/target/jpo-mec-deposit.jar /home

ENTRYPOINT ["java", \
	"-jar", \
	"/home/jpo-mec-deposit.jar"]
