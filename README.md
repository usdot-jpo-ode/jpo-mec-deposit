# jpo-mec-deposit (Message Deposit Service)

This project is intended to serve as a consumer application to subscribe to a Kafka topic of streaming JSON from the [ODE](https://github.com/usdot-jpo-ode/jpo-ode) and stream this data to MECs depending on the provided configuration. This runs alongside the ODE and when deployed using Docker Compose, runs in a Docker container.

## Table of Contents

- [jpo-mec-deposit (Message Deposit Service)](#jpo-mec-deposit-message-deposit-service)
  - [Table of Contents](#table-of-contents)
  - [Release Notes](#release-notes)
  - [Usage](#usage)
    - [Run with Docker](#run-with-docker)
    - [Docker Compose Files](#docker-compose-files)
    - [Run with Vscode](#run-with-vscode)
      - [Launch Configurations](#launch-configurations)
        - [Generate GitHub Token](#generate-github-token)
  - [Configuration](#configuration)
    - [ETX MEC Deposit](#etx-mec-deposit)
      - [ETX MQTT Deposit](#etx-mqtt-deposit)
      - [ETX API Deposit](#etx-api-deposit)
    - [Confluent Cloud Integration](#confluent-cloud-integration)
      - [Environment variables](#environment-variables)
        - [Purpose \& Usage](#purpose--usage)
        - [Note](#note)
  - [Development Setup](#development-setup)
    - [Integrated Development Environment (IDE)](#integrated-development-environment-ide)
    - [Dev Container Environment](#dev-container-environment)
    - [Checkstyle configuration](#checkstyle-configuration)
  - [Testing](#testing)
    - [Unit Tests](#unit-tests)

<!--
#############################################
############# Release Notes #############
#############################################
 -->

<a name="release-notes"></a>

## Release Notes

The current version and release history of the jpo-mec-deposit: [jpo-mec-deposit Release Notes](<docs/Release_notes.md>)

[Back to top](#table-of-contents)

<!--
#############################################
############# Usage #############
#############################################
 -->

<a name="usage"></a>

## Usage

### Run with Docker

1. Create a copy of `sample.env` and rename it to `.env`.
2. Create a copy of `jpo-utils/sample.env` and rename it to `.env` in the `jpo-utils` directory.
3. Update the variable `DOCKER_HOST_IP` to the local IP address of the system running docker in the `.env` file.
4. Generate GitHub Token
   1. Log into GitHub.
   2. Navigate to Settings -> Developer settings -> Personal access tokens.
   3. Click "New personal access token (classic)".
      1. As of now, GitHub does not support `Fine-grained tokens` for obtaining packages.
   4. Provide a name and expiration for the token.
   5. Select the `read:packages` scope.
   6. Click "Generate token" and copy the token.
   7. Copy the token name and token value into your `.env` file.
5. Run the following command to start the Docker Compose services: `docker compose up -d`

### Docker Compose Files

The following docker compose files are provided to help with development:

1. `docker-compose.yml` file can be used to spin up the depositor as a container.
2. `docker-compose-ode.yml` file can be used to spin up the depositor as a container along with the ODE.
3. `jpo-utils/docker-compose.yml` file can be used to spin up infrastructure services (kafka, mongo, etc.). Please refer to the [jpo-utils README](./jpo-utils/README.md) for more information.

To vary which services are started, use the `COMPOSE_PROFILES` environment variable. This project has a few profiles defined in the [sample.env](./sample.env) file. For further profiles from JPO Utils please refer to the [jpo-utils README](./jpo-utils/README.md) and [sample.env](./jpo-utils/sample.env).

### Run with Vscode

#### Launch Configurations

A launch.json file with some launch configurations have been included to allow developers to debug the project in VSCode. Please make sure your `.env` file is already created and populated with the correct values. Also, make sure to run docker compose up -d before running the launch configuration. Also add the following local configuration to allow for retrieval of GitHub hosted JAR files:

##### Generate GitHub Token

A GitHub token is required to pull artifacts from GitHub repositories. This is required to obtain the jpo-ode jars and must be done before attempting to build this repository.

1. Log into GitHub.
2. Navigate to Settings -> Developer settings -> Personal access tokens.
3. Click "New personal access token (classic)".
   1. As of now, GitHub does not support `Fine-grained tokens` for obtaining packages.
4. Provide a name and expiration for the token.
5. Select the `read:packages` scope.
6. Click "Generate token" and copy the token.
7. Copy the token name and token value into your `.env` file.
8. Create a copy of [settings.xml](jpo-mec-deposit/settings.xml) and save it to `~/.m2/settings.xml`
9. Update the variables in your `~/.m2/settings.xml` with the token value and target jpo-ode organization. Here is an example filled in `settings.xml` file:

```XML
<?xml version="1.0" encoding="UTF-8"?>
<settings>
    <activeProfiles>
        <activeProfile>default</activeProfile>
    </activeProfiles>
    <servers>
        <server>
            <id>github</id>
            <username>jpo_mec_deposit</username>
            <password>ghp_token-string-value</password>
        </server>
    </servers>
    <profiles>
        <profile>
            <id>default</id>
            <repositories>
                <repository>
                    <id>github</id>
                    <name>GitHub Apache Maven Packages</name>
                    <url>https://maven.pkg.github.com/usdot-jpo-ode/jpo-ode</url>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                </repository>
            </repositories>
        </profile>
    </profiles>
</settings>
```

To run the project through the launch configuration and start debugging, the developer can navigate to the Run panel (View->Run or Ctrl+Shift+D), select the configuration at the top, and click the green arrow or press F5 to begin.

[Back to top](#table-of-contents)

<!--
#############################################
############# Configuration #############
#############################################
 -->

<a name="configuration"></a>

## Configuration

### ETX MEC Deposit

The ETX MEC Deposit is a feature that allows the depositor to deposit messages to an ETX MEC. This is done by setting the `ETX_ENABLED` environment variable to `True` and providing the necessary ETX configuration. Please refer to the [sample.env](./sample.env) file for the necessary environment variables.

#### ETX MQTT Deposit

The ETX MQTT Deposit is a feature that allows the depositor to deposit messages to an ETX MQTT broker. This is done by setting the `ETX_MQTT_ENABLED` environment variable to `True` and providing the necessary ETX MQTT configuration. Please refer to the [sample.env](./sample.env) file for the necessary environment variables.

#### ETX API Deposit

The ETX API Deposit is a feature that allows the depositor to deposit messages to an ETX API. This is done by setting the `ETX_API_ENABLED` environment variable to `True` and providing the necessary ETX API configuration. Please refer to the [sample.env](./sample.env) file for the necessary environment variables.

### Confluent Cloud Integration

Rather than using a local kafka instance, this project can utilize an instance of kafka hosted by Confluent Cloud via SASL.

#### Environment variables

##### Purpose & Usage

- The `KAFKA_BOOTSTRAP_SERVERS` environment variable is used to communicate with the bootstrap server that the instance of Kafka is running on.
- The `SPRING_PROFILES_ACTIVE` environment variable specifies what type of kafka connection will be attempted and is used to check if Confluent should be utilized.
- The `CONFLUENT_KEY` and `CONFLUENT_SECRET` environment variables are used to authenticate with the bootstrap server.

##### Note

This has only been tested with Confluent Cloud but technically all SASL authenticated Kafka brokers can be reached using this method.

[Back to top](#table-of-contents)

<!--
#############################################
############# Development Setup #############
#############################################
 -->

<a name="development-setup"></a>

## Development Setup

### Integrated Development Environment (IDE)

Install the IDE of your choice:

- VSCode (Recommended): [https://code.visualstudio.com/](https://code.visualstudio.com/)
- Eclipse: [https://eclipse.org/](https://eclipse.org/)
- STS: [https://spring.io/tools/sts/all](https://spring.io/tools/sts/all)
- IntelliJ: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)

### Dev Container Environment

The project can be reopened inside a dev container in VSCode. This environment should have all the necessary dependencies to debug the ODE and its submodules. When attempting to run scripts in this environment, it may be necessary to make them executable with "chmod +x" first.

### Checkstyle configuration

This project uses [Checkstyle](https://github.com/checkstyle/checkstyle) with a modified version
of Google's Java Style guide to weakly enforce style standards. To configure Checkstyle with your
chosen IDE follow one of the following guides. This repo's checkstyle configuration file can be found
[here](checkstyle.xml). For a quick guide to Checkstyle, check out this short [article](https://www.baeldung.com/checkstyle-java).

- [Intellij](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea)
- [VSCode](https://code.visualstudio.com/docs/java/java-linting#_checkstyle)
- [Eclipse](https://checkstyle.org/eclipse-cs/#!/project-setup)

If you prefer the command line for your checkstyle output. You can run `mvn checkstyle:check` to
check the whole project. See [Checkstyle's Github](https://github.com/checkstyle/checkstyle) for more info.

[Back to top](#table-of-contents)

## Testing

### Unit Tests

To run the unit tests, reopen the project in the provided dev container and run the following command:

``` bash
cd jpo-mec-deposit
mvn test
```

This will run the unit tests and provide a report of the results.
