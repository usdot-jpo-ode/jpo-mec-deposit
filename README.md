# jpo-mec-deposit (Message Deposit Service)

This project is intended to serve as a consumer application to subscribe to a Kafka topic of streaming JSON from the [ODE](https://github.com/usdot-jpo-ode/jpo-ode) and stream this data to MECs depending on the provided configuration. This runs alongside the ODE and when deployed using Docker Compose, runs in a Docker container.

## Table of Contents

- [Release Notes](#release-notes)
- [Usage](#usage)
- [Configuration](#configuration)
- [Development Setup](#development-setup)
- [Testing](#testing)

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
4. Run the following command to start the Docker Compose services: `docker compose up -d`

### Docker Compose Files

The following docker compose files are provided to help with development:

1. `docker-compose.yml` file can be used to spin up the depositor as a container.
2. `docker-compose-ode.yml` file can be used to spin up the depositor as a container along with the ODE.
3. `jpo-utils/docker-compose.yml` file can be used to spin up infrastructure services (kafka, mongo, etc.). Please refer to the [jpo-utils README](./jpo-utils/README.md) for more information.

To vary which services are started, use the `COMPOSE_PROFILES` environment variable. This project has a few profiles defined in the [sample.env](./sample.env) file. For further profiles from JPO Utils please refer to the [jpo-utils README](./jpo-utils/README.md) and [sample.env](./jpo-utils/sample.env).

### Run with Vscode

#### Launch Configurations

A launch.json file with some launch configurations have been included to allow developers to debug the project in VSCode. Please make sure your `.env` file is already created and populated with the correct values. Also, make sure to run docker compose up -d before running the launch configuration.

To run the project through the launch configuration and start debugging, the developer can navigate to the Run panel (View->Run or Ctrl+Shift+D), select the configuration at the top, and click the green arrow or press F5 to begin.

[Back to top](#table-of-contents)

<!--
#############################################
############# Configuration #############
#############################################
 -->

<a name="configuration"></a>

## Configuration

### Confluent Cloud Integration

Rather than using a local kafka instance, this project can utilize an instance of kafka hosted by Confluent Cloud via SASL.

#### Environment variables

##### Purpose & Usage

- The `KAFKA_BOOTSTRAP_SERVERS` environment variable is used to communicate with the bootstrap server that the instance of Kafka is running on.
- The `SPRING_PROFILES_ACTIVE` environment variable specifies what type of kafka connection will be attempted and is used to check if Confluent should be utilized.
- The `CONFLUENT_KEY` and `CONFLUENT_SECRET` environment variables are used to authenticate with the bootstrap server.

##### Note

This has only been tested with Confluent Cloud but technically all SASL authenticated Kafka brokers can be reached using this method.

### ETX MEC Deposit

The ETX MEC Deposit is a feature that allows the depositor to deposit messages to an ETX MEC. This is done by setting the `ETX_ENABLED` environment variable to `True` and providing the necessary ETX configuration. Please refer to the [sample.env](./sample.env) file for the necessary environment variables.

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
