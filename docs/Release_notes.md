JPO MEC Deposit Release Notes
----------------------------

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
