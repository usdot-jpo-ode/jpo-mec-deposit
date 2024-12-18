package us.dot.its.jpo.ode.mec.deposit.imp;

import us.dot.its.jpo.ode.mec.deposit.DateJsonMapper;
import us.dot.its.jpo.ode.mec.deposit.DepositorProperties;
import us.dot.its.jpo.ode.mec.deposit.utils.CommonUtils;
import us.dot.its.jpo.ode.mec.deposit.models.imp.ConfigData;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthToken;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.AuthTokenRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientCompleteResponse;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientConnectionPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientConnectionResponse;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientRegistrationConnectionPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientRegistrationPostRequest;
import us.dot.its.jpo.ode.mec.deposit.models.imp.partner.ClientRegistrationResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class ImpPartnerApi {

    private DepositorProperties properties;
    private ObjectMapper objectMapper;
    private RestTemplate restTemplate;

    public ImpPartnerApi(DepositorProperties depositorProperties) {
        this.properties = depositorProperties;
        this.restTemplate = new RestTemplate();
        this.objectMapper = DateJsonMapper.getInstance();
        this.restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    }

    public ConfigData registerClientPartner() {
        try {
            ConfigData configData;
            String deviceID = null;
            Boolean cacheRegistration = properties.getImpCacheRegistration();

            String configPath = properties.getImpCertPath() + "/config.json";
            String caCertPath = properties.getImpCertPath() + "/imp-ca.pem";
            String certPath = properties.getImpCertPath() + "/imp-cert.pem";
            String keyPath = properties.getImpCertPath() + "/imp-key.pem";

            long startTime = System.currentTimeMillis();
            String token = getToken();
            long endtime = System.currentTimeMillis();
            log.info("Time to get token: " + (endtime - startTime) + " ms");

            if (!validRegistration(configPath) || !cacheRegistration) {
                log.info("Registering client partner");

                startTime = System.currentTimeMillis();
                ClientRegistrationResponse registrationResponse = register(token);
                endtime = System.currentTimeMillis();
                log.info("Time to register: " + (endtime - startTime) + " ms");

                CommonUtils.writeToFile(caCertPath, registrationResponse.getCertificate().getCaPem());
                CommonUtils.writeToFile(certPath, registrationResponse.getCertificate().getCertPem());
                CommonUtils.writeToFile(keyPath, registrationResponse.getCertificate().getKeyPem());

                deviceID = registrationResponse.getDeviceID();

                startTime = System.currentTimeMillis();
                ClientConnectionResponse connectionResponse = connection(token, deviceID);
                endtime = System.currentTimeMillis();
                log.info("Time to connect: " + (endtime - startTime) + " ms");

                URI uri = new URI(connectionResponse.getMqttURL());
                configData = new ConfigData(configPath, caCertPath, certPath, keyPath, properties.getImpVendor(),
                        properties.getImpNetworkType(), uri, deviceID);

                String configDataJson = objectMapper.writeValueAsString(configData);

                CommonUtils.writeToFile(configPath, configDataJson);
            } else {
                log.info("Client partner already registered, obtaining latest connection string");

                String configDataJson = Files.readString(Paths.get(configPath));
                configData = objectMapper.readValue(configDataJson, ConfigData.class);
                deviceID = configData.getDeviceID();

                startTime = System.currentTimeMillis();
                ClientConnectionResponse connectionResponse = connection(token, deviceID);
                endtime = System.currentTimeMillis();
                log.info("Time to connect: " + (endtime - startTime) + " ms");

                URI uri = new URI(connectionResponse.getMqttURL());
                configData.setImpMqttUri(uri);
            }

            return configData;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public String getToken() {
        var request = new AuthTokenRequest(properties.getImpPartnerUser(), properties.getImpPartnerPass());

        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");

        HttpEntity<AuthTokenRequest> entity = new HttpEntity<>(request, headers);

        AuthToken response = restTemplate.postForObject(properties.getImpPartnerApiBaseUri() + "/auth/token", entity,
                AuthToken.class);

        return response.getAccessToken();
    }

    public ClientRegistrationResponse register(String token) {
        ClientRegistrationPostRequest request = new ClientRegistrationPostRequest(
                properties.getImpClientType().getValue(), properties.getImpClientSubType().getValue());

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.set("Content-Type", "application/json");

        HttpEntity<ClientRegistrationPostRequest> entity = new HttpEntity<>(request, headers);

        try {
            ResponseEntity<ClientRegistrationResponse> response = restTemplate.exchange(
                    properties.getImpPartnerApiBaseUri() + "/prd/v2/registration", HttpMethod.POST, entity,
                    ClientRegistrationResponse.class);

            return response.getBody();
        } catch (HttpClientErrorException.Unauthorized e) {
            log.error("Unauthorized error: " + e.getStackTrace());
            return null;
        }
    }

    public ClientConnectionResponse connection(String token, String deviceID) {
        ClientConnectionPostRequest request = new ClientConnectionPostRequest(deviceID, properties.getImpMecLatitude(),
                properties.getImpMecLongitude(), properties.getImpNetworkType());

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + token);
        headers.set("Content-Type", "application/json");

        HttpEntity<ClientConnectionPostRequest> entity = new HttpEntity<>(request, headers);

        try {
            ResponseEntity<ClientConnectionResponse> response = restTemplate.exchange(
                    properties.getImpPartnerApiBaseUri() + "/prd/v2/connection", HttpMethod.POST, entity,
                    ClientConnectionResponse.class);

            return response.getBody();
        } catch (HttpClientErrorException.Unauthorized e) {
            log.error("Unauthorized error: " + e.getStackTrace());
            return null;
        }
    }

    public boolean validRegistration(String configPath) {
        boolean valid = false;
        File file = new File(configPath);
        if (file.exists()) {
            try {
                String configDataJson = Files.readString(Paths.get(configPath));
                ConfigData configData = objectMapper.readValue(configDataJson, ConfigData.class);

                if (configData.getDeviceID() != null && configData.getNetworkType() == properties.getImpNetworkType()) {
                    valid = true;
                }
            } catch (IOException e) {
                log.error("validRegistration error: " + e.getStackTrace());
            }
        }
        return valid;
    }
}