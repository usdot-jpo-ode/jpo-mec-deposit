package us.dot.its.jpo.ode.mec.deposit.services.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 * Validates signed payloads for NMI/AV publishing via the local signature validation API.
 */
@Slf4j
@Service
public class SignedMessageValidationService {
  private final RestTemplate restTemplate;
  private final ObjectMapper objectMapper;
  private final String validationApiUrl;
  private final boolean enabled;

  /**
   * Creates a signature validation service backed by the external validation API.
   */
  public SignedMessageValidationService(RestTemplate restTemplate, ObjectMapper objectMapper,
      @Value("${mec-deposit.mqtt-signature-validation.api-url:http://127.0.0.1:8088/validate}") String validationApiUrl,
      @Value("${mec-deposit.mqtt-signature-validation.enabled:true}") boolean enabled) {
    this.restTemplate = restTemplate;
    this.objectMapper = objectMapper;
    this.validationApiUrl = validationApiUrl;
    this.enabled = enabled;
  }

  /**
   * For signed payloads, validates signature status and PSID consistency against target topics.
   */
  public void validateIfSigned(byte[] payload, Set<String> topics, String brokerLabel) {
    if (!enabled || payload == null || payload.length == 0 || topics == null || topics.isEmpty()) {
      return;
    }

    String messageHex = Hex.toHexString(payload).toUpperCase();
    ValidationRequest request = new ValidationRequest();
    request.setMessageHex(messageHex);
    request.setShouldValidate(false);
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(java.util.List.of(MediaType.ALL));
    String requestJson;
    try {
      requestJson = objectMapper.writeValueAsString(request);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize signature validation request body", e);
    }
    HttpEntity<String> entity = new HttpEntity<>(requestJson, headers);

    if (log.isDebugEnabled()) {
      log.debug(
          "{} signature validation request -> url={}, payloadBytes={}, topics={}, messageHexPrefix={}",
          brokerLabel, validationApiUrl, payload.length, topics, messageHex.substring(0, Math.min(24, messageHex.length())));
    }

    ResponseEntity<JsonNode> response;
    JsonNode body;
    try {
      response = restTemplate.postForEntity(validationApiUrl, entity, JsonNode.class);
      body = response.getBody();
    } catch (RestClientException e) {
      throw new IllegalStateException(
          String.format("%s signature validation request failed at %s: %s",
              brokerLabel, validationApiUrl, e.getMessage()),
          e);
    }

    if (log.isDebugEnabled()) {
      log.debug("{} signature validation response <- statusCode={}, bodyPresent={}", brokerLabel,
          response.getStatusCode(), body != null);
    }
    if (body == null) {
      throw new IllegalStateException("Signature validation API returned empty response");
    }

    JsonNode signedData = body.path("decodedMessage").path("content").path("signedData");
    if (signedData.isMissingNode() || signedData.isNull()) {
      log.debug("{} payload is not signed, skipping signature checks", brokerLabel);
      return;
    }

    String status = body.path("status").asText("");
    if (!"VALID".equalsIgnoreCase(status)) {
      throw new IllegalStateException(
          String.format("%s signature validation failed with status: %s", brokerLabel, status));
    }
    log.debug("{} signature validation passed with status {}", brokerLabel, status);

    String headerPsid = signedData.path("tbsData").path("headerInfo").path("psid").asText("");
    if (headerPsid.isBlank()) {
      throw new IllegalStateException(
          String.format("%s signed payload missing header PSID", brokerLabel));
    }

    Set<String> allowedPsids = extractAllowedPsids(signedData);
    if (!allowedPsids.isEmpty() && !allowedPsids.contains(headerPsid)) {
      throw new IllegalStateException(
          String.format("%s signed payload PSID %s not in certificate permissions %s", brokerLabel,
              headerPsid, allowedPsids));
    }

    for (String topic : topics) {
      String topicPsid = topic.substring(topic.lastIndexOf('/') + 1);
      if (!topicPsid.matches("\\d+")) {
        continue;
      }
      if (!headerPsid.equals(topicPsid)) {
        throw new IllegalStateException(
            String.format("%s topic PSID mismatch for %s (topic=%s, signed=%s)", brokerLabel, topic,
                topicPsid, headerPsid));
      }
    }
  }

  private static Set<String> extractAllowedPsids(JsonNode signedData) {
    JsonNode appPermissions = signedData.path("signer").path("certificate").path(0)
        .path("toBeSigned").path("appPermissions");
    if (!appPermissions.isArray()) {
      return Set.of();
    }
    return stream(appPermissions).map(n -> n.path("psid").asText(""))
        .filter(psid -> !psid.isBlank()).collect(Collectors.toSet());
  }

  private static java.util.stream.Stream<JsonNode> stream(JsonNode node) {
    java.util.Iterator<JsonNode> iterator = node.elements();
    return iterator == null ? java.util.stream.Stream.empty()
        : java.util.stream.StreamSupport
            .stream(java.util.Spliterators.spliteratorUnknownSize(iterator, 0), false);
  }

  /** Request payload model expected by the local /validate API. */
  public static class ValidationRequest {
    private String messageHex;
    private Boolean shouldValidate;

    public ValidationRequest() {
    }

    public String getMessageHex() {
      return messageHex;
    }

    public void setMessageHex(String messageHex) {
      this.messageHex = messageHex;
    }

    public Boolean getShouldValidate() {
      return shouldValidate;
    }

    public void setShouldValidate(Boolean shouldValidate) {
      this.shouldValidate = shouldValidate;
    }
  }
}
