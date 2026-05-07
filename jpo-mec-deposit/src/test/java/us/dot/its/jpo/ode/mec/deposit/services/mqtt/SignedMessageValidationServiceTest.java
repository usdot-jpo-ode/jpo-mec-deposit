package us.dot.its.jpo.ode.mec.deposit.services.mqtt;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
class SignedMessageValidationServiceTest {

  @Mock
  private RestTemplate restTemplate;

  private SignedMessageValidationService service;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service = new SignedMessageValidationService(restTemplate, mapper,
        "http://127.0.0.1:8088/validate", true);
  }

  @Test
  void validateIfSigned_unsignedPayload_skipsValidation() throws Exception {
    String body = """
        {"decodedMessage":{"content":{}},"status":"VALID"}
        """;
    when(restTemplate.postForEntity(eq("http://127.0.0.1:8088/validate"), any(), eq(com.fasterxml.jackson.databind.JsonNode.class)))
        .thenReturn(new ResponseEntity<>(mapper.readTree(body), HttpStatus.OK));

    assertDoesNotThrow(() -> service.validateIfSigned(new byte[] {1, 2}, Set.of("/v1/g32/a/b/c/130"), "NMI"));
  }

  @Test
  void validateIfSigned_signedPayloadValidAndMatchingPsid_passes() throws Exception {
    String body = """
        {
          "decodedMessage": {
            "content": {
              "signedData": {
                "signer": {
                  "certificate": [
                    { "toBeSigned": { "appPermissions": [ { "psid": 130 }, { "psid": 131 } ] } }
                  ]
                },
                "tbsData": { "headerInfo": { "psid": 130 } }
              }
            }
          },
          "status": "VALID"
        }
        """;
    when(restTemplate.postForEntity(eq("http://127.0.0.1:8088/validate"), any(), eq(com.fasterxml.jackson.databind.JsonNode.class)))
        .thenReturn(new ResponseEntity<>(mapper.readTree(body), HttpStatus.OK));

    assertDoesNotThrow(
        () -> service.validateIfSigned(new byte[] {3, 4}, Set.of("/v1/g32/x/y/z/130"), "NMI"));
  }

  @Test
  void validateIfSigned_signedPayloadTopicPsidMismatch_throws() throws Exception {
    String body = """
        {
          "decodedMessage": {
            "content": {
              "signedData": {
                "signer": {
                  "certificate": [
                    { "toBeSigned": { "appPermissions": [ { "psid": 130 }, { "psid": 131 } ] } }
                  ]
                },
                "tbsData": { "headerInfo": { "psid": 130 } }
              }
            }
          },
          "status": "VALID"
        }
        """;
    when(restTemplate.postForEntity(eq("http://127.0.0.1:8088/validate"), any(), eq(com.fasterxml.jackson.databind.JsonNode.class)))
        .thenReturn(new ResponseEntity<>(mapper.readTree(body), HttpStatus.OK));

    assertThrows(IllegalStateException.class,
        () -> service.validateIfSigned(new byte[] {5, 6}, Set.of("/v1/g32/x/y/z/131"), "AV"));
  }
}
