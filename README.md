![GitHub Release](https://img.shields.io/github/v/release/usdot-jpo-ode/jpo-mec-deposit) [![CI](https://github.com/usdot-jpo-ode/jpo-mec-deposit/actions/workflows/ci.yml/badge.svg)](https://github.com/usdot-jpo-ode/jpo-mec-deposit/actions/workflows/ci.yml) ![Docker Pulls](https://img.shields.io/docker/pulls/usdotjpoode/jpo-mec-deposit) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=usdot-jpo-ode_jpo-mec-deposit&metric=alert_status)](https://sonarcloud.io/dashboard?id=usdot-jpo-ode_jpo-mec-deposit) ![GitHub License](https://img.shields.io/github/license/usdot-jpo-ode/jpo-ode)


# jpo-mec-deposit

This project is intended to serve as a consumer application to subscribe to a Kafka topic of streaming JSON from the [ODE](https://github.com/usdot-jpo-ode/jpo-ode) and stream this data to MQTT based Mobile Edge Compute (MEC) datacenters depending on the provided configuration to allow for network based V2X integrations. This runs alongside the ODE and when deployed using Docker Compose, runs in a Docker container.

## Table of Contents

- [jpo-mec-deposit](#jpo-mec-deposit)
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
  - [Message Flow Diagrams](#message-flow-diagrams)
    - [ODE MQTT Publisher Message Flow](#ode-mqtt-publisher-message-flow)
    - [ODE API Publisher Message Flow](#ode-api-publisher-message-flow)
    - [GeoHash MQTT Publisher Message Flow](#geohash-mqtt-publisher-message-flow)
    - [V2X App API Integration](#v2x-app-api-integration)
  - [GeoHash MQTT Publisher](#geohash-mqtt-publisher)
    - [Overview](#overview)
    - [Architecture](#architecture)
    - [Message Processing Flow](#message-processing-flow)
    - [Publisher Configuration](#publisher-configuration)
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
############# Message Flow Diagrams #############
#############################################
-->

<a name="message-flow-diagrams"></a>

## Message Flow Diagrams

The following diagrams illustrate the different message flow patterns supported by jpo-mec-deposit for publishing V2X messages to ETX MEC platforms.

### ODE MQTT Publisher Message Flow

The ODE MQTT Publisher flow handles direct publishing of ODE processed messages (BSM, TIM, SPaT, SDSM, etc.) to the Verizon MEC MQTT Server.

![ODE MQTT Publisher Message Flow](docs/ode-mqtt-publisher-diagram.png)

**Key Steps:**

1. **Get Keycloak Token**: The jpo-mec-deposit consumer requests a Keycloak token from the Partner Backend API for authentication.
2. **Request Certificate + MQTT URL**: The consumer requests the necessary certificate and MQTT URL from the Partner Backend API.
3. **ODE Processed Messages**: JPO ODE sends processed messages (BSM, TIM, SPaT, SDSM, etc.) to the jpo-mec-deposit protobuf consumer.
4. **MQTT Regional Publish**: The consumer publishes messages directly to the Verizon MEC MQTT Server using MQTT.

The Partner Backend API handles authentication with Keycloak, retrieves certificates and MQTT URLs from the Verizon ETX Registration API, and manages logging and TIM configuration in PostgreSQL.

### ODE API Publisher Message Flow

The ODE API Publisher flow handles publishing messages via the ETX Configuration API, allowing for geofence-based message deployment.

![ODE API Publisher Message Flow](docs/ode-api-publisher-diagram.png)

**Key Steps:**

1. **Get Keycloak Token**: The jpo-mec-deposit consumer requests a Keycloak token from the Partner Backend API.
2. **Request Certificate + MQTT URL**: The consumer requests certificate and MQTT URL from the Partner Backend API.
3. **ODE Processed Messages**: JPO ODE sends processed messages (TIM & MAP) to the jpo-mec-deposit protobuf consumer.
4. **API Deployed Message Payload**: The Partner Backend API deploys the message payload to the Verizon MEC MQTT Server.
5. **Deploy Configuration with Geofence**: The Partner Backend API deploys a configuration with geofence for the message payload to the Verizon ETX Configuration API.

This flow enables region-based message publishing through the Configuration API, which then publishes V2X messages for the defined region to the MQTT Server.

### GeoHash MQTT Publisher Message Flow

The GeoHash MQTT Publisher flow handles publishing V2X messages using geohash-based routing through Kafka and MQTT.

![GeoHash MQTT Publisher Message Flow](docs/geohash-mqtt-publisher-diagram.png)

**Key Steps:**

1. **Get Keycloak Token**: The jpo-mec-deposit consumer requests a Keycloak token from the Partner Backend API.
2. **Request Certificate + MQTT URL**: The consumer requests certificate and MQTT URL from the Partner Backend API.
3. **Publishing V2X Messages**: V2X messages are published in `geoHashRoutedMsg` protobuf definitions to a Config Kafka publisher.
4. **MQTT Publish (TIM)**: The consumer publishes messages to the Verizon MEC MQTT Server using MQTT.

The Partner Backend API manages authentication, certificate retrieval, and interacts with PostgreSQL for logging, TIM configuration, and user database operations. PostgreSQL publishes TIM deployment configurations to the Config Kafka publisher, which feeds into the geohash routing system.

### V2X App API Integration

The V2X App API serves as the central application interface for authentication, registration, and data management in the V2X ecosystem. jpo-mec-deposit integrates with the V2X App API to obtain authentication tokens, ETX certificates, and MQTT connection details.

![V2X App API Architecture](docs/v2x-api-diagram.png)

**Key Components:**

- **V2x App API**: Central hub that provides:
  - Authentication services via Keycloak integration
  - ETX certificate and MQTT URL retrieval from Verizon ETX Registration API
  - TIM mappings and configuration management
  - Logging and configuration storage in PostgreSQL

- **Integration Points with jpo-mec-deposit**:
  - **Get Keycloak Token**: jpo-mec-deposit requests authentication tokens from the V2X App API
  - **Request/Validate ETX Certificate + MQTT URL**: jpo-mec-deposit obtains necessary credentials and endpoint information for connecting to the Verizon MEC MQTT Server

- **Supporting Services**:
  - **Keycloak**: Identity and access management, authenticates requests from V2X App API
  - **PostgreSQL**: Stores logging data, TIM configuration, and serves as Keycloak's backing user database
  - **Verizon ETX Registration API**: Provides certificate and MQTT URL retrieval
  - **TIM Kafka Publisher**: Publishes geohash-routed messages consumed by jpo-mec-deposit

**Message Flow:**

The V2X App API orchestrates the authentication and registration flow:

1. jpo-mec-deposit requests a Keycloak token from the V2X App API
2. V2X App API authenticates with Keycloak (which uses PostgreSQL for user data)
3. jpo-mec-deposit requests ETX certificate and MQTT URL from the V2X App API
4. V2X App API retrieves credentials from the Verizon ETX Registration API
5. jpo-mec-deposit uses the obtained credentials to publish messages to the Verizon MEC MQTT Server

For more detailed information about the V2X App API, including setup, configuration, and API documentation, please refer to the [V2X App API GitHub repository](https://github.com/usdot-fhwa-stol/v2x-app-api).

[Back to top](#table-of-contents)

<!--
#############################################
############# GeoHash MQTT Publisher #############
#############################################
-->

<a name="geohash-mqtt-publisher"></a>

## GeoHash MQTT Publisher

### Overview

The GeoHash MQTT Publisher (`EtxGeohashMqttPublisher`) is a specialized component that consumes geohash-routed protobuf messages from Kafka and publishes them as geo-routed messages to MQTT topics. This publisher enables efficient geographic routing of V2X messages using geohash-based topic organization.

### Architecture

The GeoHash MQTT Publisher operates as a Kafka consumer that:

- **Consumes**: `GeoHashRoutedMsg` protobuf messages from a configured Kafka topic
- **Transforms**: Converts `GeoHashRoutedMsg` to `GeoRoutedMsg` protobuf format
- **Publishes**: Sends messages to MQTT topics organized by geohash and message type

**Key Components:**

- **GeoHashRoutedMsg**: Input protobuf message containing:
  - Original message bytes (ASN.1 encoded V2X message)
  - Timestamp
  - Geohash string (base32 encoded geographic identifier)

- **GeoRoutedMsg**: Output protobuf message containing:
  - Original message bytes
  - Timestamp
  - Position (latitude/longitude derived from geohash)

### Message Processing Flow

1. **Kafka Consumption**: The publisher listens to the configured Kafka topic (default: `topic.GeoHashRoutedMsg`) using a byte array consumer.

2. **Message Parsing**:
   - Parses the incoming `GeoHashRoutedMsg` protobuf message
   - Extracts the original message bytes and geohash string

3. **Geohash Processing**:
   - Converts the geohash string to a `GeoHash` object
   - Extracts latitude and longitude coordinates from the geohash originating point

4. **Message Type Detection**:
   - Analyzes the original message bytes to detect the V2X message type (BSM, TIM, SPaT, MAP, SDSM, etc.)
   - Defaults to TIM if detection fails

5. **Topic Generation**:
   - Builds MQTT topic using the geohash, detected message type, and configuration properties
   - Topic structure includes: namespace (REGIONAL or REGIONAL_STATIC), geohash path, message type, vendor, client type/subtype, and message format
   - Uses geohash directly to avoid redundant coordinate conversions

6. **Protobuf Construction**:
   - Builds a `GeoRoutedMsg` protobuf message containing:
     - Original message bytes
     - Timestamp (UTC)
     - Position (latitude/longitude from geohash)

7. **MQTT Publishing**:
   - Publishes the `GeoRoutedMsg` as ASN.1 bytes to the generated MQTT topic
   - Uses configured QoS and retain settings

8. **Metrics & Logging**:
   - Records processing success/failure metrics
   - Logs debug information including topic, message type, and geohash

### Publisher Configuration

The GeoHash MQTT Publisher is enabled by setting the following environment variables:

```bash
# Enable ETX functionality
ETX_ENABLED="True"

# Enable GeoHash MQTT Publisher
ETX_DEPOSITORS_GEOHASH_MQTT_ENABLED="True"

# Kafka topic for GeoHashRoutedMsg messages
ETX_DEPOSITORS_GEOHASH_MQTT_KAFKA_TOPIC="topic.GeoHashRoutedMsg"
```

**Additional Configuration Properties:**

- `mec-deposit.etx.mqtt.precision`: Geohash precision (default: 7, range: 6-8)
- `mec-deposit.etx.mqtt.vendor`: Vendor identifier for MQTT topics
- `mec-deposit.etx.mqtt.message-format`: Message format (e.g., "j2735")
- `mec-deposit.etx.client-type`: Client type (e.g., "Software")
- `mec-deposit.etx.client-sub-type`: Client subtype (e.g., "Application")

**Conditional Activation:**

The publisher is only activated when both of the following conditions are met:

- `mec-deposit.etx.depositors.geohash.mqtt.enabled=true`
- `mec-deposit.etx.enabled=true`

**Kafka Consumer Group:**

The publisher uses a dedicated consumer group: `${spring.kafka.consumer.group-id}-geohash-mqtt-publisher`

This ensures that geohash messages are processed independently from other message types and allows for separate scaling and monitoring.

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
