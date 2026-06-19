JPO MEC Deposit Release Notes
----------------------------

Version 2.0.0
----------------------------------------

### **Summary**

Multi-broker MQTT fanout support and depositor layer refactor. The main updates in this version include:

- **Multi-broker MQTT fanout**: messages are now fanned out simultaneously to ETX, NMI, AV, and MB broker targets via the new `MultiBrokerPublishService` and `BrokerPublisher` interface.
- **New broker publish services**: full publish service implementations for NMI (`NmiMqttPublishService`), AV (`AvMqttPublishService`), and MB (`MbMqttPublishService`), each with independent connection management and circuit breaker logic.
- **New PSM MQTT depositor**: `PsmMqttDepositor` with multi-broker fanout support.
- **New MAP MQTT depositor tests**: `MapMqttDepositor` fully covered with unit tests.
- **Geohash publisher enhancements**: topic-building logic moved into `GeohashMqttPublisher` with comprehensive unit tests added.
- **TIM MQTT depositor**: geofence preview functionality added.
- **Depositor refactor**: all depositor classes moved from `services/etx/depositors/` to `services/depositors/` and renamed to remove the `Etx` prefix (e.g. `EtxBsmMqttDepositor` → `BsmMqttDepositor`). `@KafkaListener` topics and feature-flag property keys are unchanged.
- **Abstract base class rename**: `AbstractEtxDepositor` → `AbstractDepositor`, `AbstractEtxMqttDepositor` → `AbstractMqttDepositor`, `AbstractEtxApiDepositor` → `AbstractApiDepositor`.
- **Partner API rename**: `EtxPartnerClient` → `PartnerClient`, `EtxPartnerApiProperties` → `PartnerApiProperties`, `EtxTokenManager` → `PartnerTokenManager`.
- **Feature flags**: `@ConditionalOnAnyMqttBrokerMqttDepositor` annotation introduced to gate each depositor bean on its per-broker configuration.
- **Prometheus metrics**: actuator endpoint enabled and configured.
- **Message signature detection**: `MessageTypeDetector` enhanced to handle signed messages.
- **Build fix**: `maven-dependency-plugin` added to unpack protobuf WKT protos before compilation, resolving `PROTOC FAILED` errors on `mvn clean` builds.
- **jpo-ode dependency**: bumped from 5.1.0 to 6.0.0.

## **Pull Requests**

- [PR#](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/): Multi-broker MQTT fanout and depositor refactor

----------------------------------------

Version 1.0.0
----------------------------------------

### **Summary**

First public release of JPO-MEC-DEPOSIT. The main updates in this version includes:

- Removal of JAR files for ODE library references in favor of using GitHub Artifacts.
- Intersection Filtering Configuration to allow for selecting only certain intersections for the ETX depositor.
- Additional MQTT MEC Deposit application for SDSM messages to the ETX.

## **Pull Requests**

- [PR5](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/5): Intersection Filtering Configuration
- [PR6](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/6): SDSM Depositor
- [PR7](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/7): JPO ODE GitHub artifacts and schema updates.

----------------------------------------

Version 0.1.0
----------------------------------------

### **Summary**

The main updates include:

- Spring Boot initial application structure
- Rate Limiting Thread Safety
- Fix ETX Clear TIM feature flag
- Rich feature flag configuration of the application
- Optional Kafka authentication configuration
- Initial MEC Deposit application for MAP, SPaT, TIM, and BSM messages to the ETX.
  - API Deposit support - meant for long lasting messages (e.g. MAP, TIM, etc.)
  - MQTT Deposit support - meant for short lasting messages (e.g. BSM, SPaT, etc.)

## **Pull Requests**

- [PR1](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/1): Repository Init Structure
- [PR2](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/2): Fix ETX Clear TIM feature flag
- [PR3](https://github.com/usdot-jpo-ode/jpo-mec-deposit/pull/3): Rate Limiting Thread Safety
