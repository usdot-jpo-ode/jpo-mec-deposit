package us.dot.its.jpo.ode.mec.deposit;

import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import us.dot.its.jpo.ode.mec.deposit.config.SystemConfig;

/**
 * Main application class for the MEC Deposit service.
 */
@SpringBootApplication(exclude = {
    org.springdoc.core.configuration.SpringDocDataRestConfiguration.class
})
@EnableKafka
@EnableScheduling
@EnableConfigurationProperties
public class MecDepositApplication {
  static final int DEFAULT_NO_THREADS = 10;
  static final String DEFAULT_SCHEMA = "default";

  /**
   * Application entry point.
   *
   * @param args Command line arguments
   * @throws MalformedObjectNameException   If MBean object name is invalid
   * @throws InterruptedException           If thread is interrupted
   * @throws InstanceAlreadyExistsException If MBean instance already exists
   * @throws MBeanRegistrationException     If MBean registration fails
   * @throws NotCompliantMBeanException     If MBean is not compliant
   */
  public static void main(String[] args) throws MalformedObjectNameException, InterruptedException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {

    SpringApplication.run(MecDepositApplication.class, args);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    SystemConfig systemConfig = new SystemConfig(DEFAULT_NO_THREADS, DEFAULT_SCHEMA);
    ObjectName name = new ObjectName("us.dot.its.jpo.ode.mec.deposit:type=SystemConfig");
    mbs.registerMBean(systemConfig, name);
  }

}
