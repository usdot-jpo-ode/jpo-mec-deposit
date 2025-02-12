JPO MEC Deposit Release Notes
----------------------------

Version 0.1.0
----------------------------------------

### **Summary**

The first release for the jpo-mec-deposit, version 0.1.0, includes a fully functioning MEC Deposit for MAP, SPaT, TIM, and BSM messages. The jpo-mec-deposit consumes jpo-ode output topics and can then deposit them to the ETX. The jpo-mec-deposit can use local Kafka or Confluent Cloud SASL authentication.Read more about the jpo-mec-deposit in the [main README](<../README.md>).

Enhancements in this release:

- PR3: Rate Limiting Thread Safety
- PR2: Fix ETX Clear TIM feature flag
- PR1: Initial MEC Deposit application for MAP, SPaT, TIM, and BSM messages to the ETX.
