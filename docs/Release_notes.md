JPO MEC Deposit Release Notes
----------------------------

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

- [PR3](https://github.com/usdot-fhwa-stol/v2x-mec/pull/3): V2 python scripts for the ETX
- [PR11](https://github.com/usdot-fhwa-stol/v2x-mec/pull/11): TIM Messages and MongoDB Latency Analysis
