package us.dot.its.jpo.ode.mec.deposit.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class for setting up the RestTemplate bean. This class provides a configured
 * RestTemplate instance for making HTTP requests with JSON message conversion capabilities.
 */
@Configuration
public class RestTemplateConfig {

  /**
   * Creates and configures a RestTemplate bean with JSON message conversion support.
   *
   * @return A configured RestTemplate instance with MappingJackson2HttpMessageConverter
   */
  @Bean
  public RestTemplate restTemplate() {
    RestTemplate template = new RestTemplate();
    template.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    return template;
  }
}
