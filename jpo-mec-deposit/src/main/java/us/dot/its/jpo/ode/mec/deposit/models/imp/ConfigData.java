package us.dot.its.jpo.ode.mec.deposit.models.imp;

import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Objects;

public class ConfigData implements Serializable {

    private String configFilePath;
    private String caCertPath;
    private String clientCertPath;
    private String keyFilePath;
    private String impVendor;
    private NetworkType networkType;
    private URI impMqttUri;
    private String deviceID;

    public ConfigData() {
    }

    public ConfigData(String configFilePath, String caCertPath, String clientCertPath, String keyFilePath,
            String impVendor, NetworkType networkType, URI impMqttUri, String deviceID) {
        this.configFilePath = configFilePath;
        this.caCertPath = caCertPath;
        this.clientCertPath = clientCertPath;
        this.keyFilePath = keyFilePath;
        this.impVendor = impVendor;
        this.networkType = networkType;
        this.impMqttUri = impMqttUri;
        this.deviceID = deviceID;
    }

    public String getConfigFilePath() {
        return this.configFilePath;
    }

    public void setConfigFilePath(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    public String getCaCertPath() {
        return this.caCertPath;
    }

    public void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }

    public String getClientCertPath() {
        return this.clientCertPath;
    }

    public void setClientCertPath(String clientCertPath) {
        this.clientCertPath = clientCertPath;
    }

    public String getKeyFilePath() {
        return this.keyFilePath;
    }

    public void setKeyFilePath(String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    public String getImpVendor() {
        return this.impVendor;
    }

    public void setImpVendor(String impVendor) {
        this.impVendor = impVendor;
    }

    public NetworkType getNetworkType() {
        return this.networkType;
    }

    public void setNetworkType(NetworkType networkType) {
        this.networkType = networkType;
    }

    public URI getImpMqttUri() {
        return this.impMqttUri;
    }

    public void setImpMqttUri(URI impMqttUri) {
        this.impMqttUri = impMqttUri;
    }

    public String getDeviceID() {
        return this.deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public ConfigData configFilePath(String configFilePath) {
        setConfigFilePath(configFilePath);
        return this;
    }

    public ConfigData caCertPath(String caCertPath) {
        setCaCertPath(caCertPath);
        return this;
    }

    public ConfigData clientCertPath(String clientCertPath) {
        setClientCertPath(clientCertPath);
        return this;
    }

    public ConfigData keyFilePath(String keyFilePath) {
        setKeyFilePath(keyFilePath);
        return this;
    }

    public ConfigData impVendor(String impVendor) {
        setImpVendor(impVendor);
        return this;
    }

    public ConfigData networkType(NetworkType networkType) {
        setNetworkType(networkType);
        return this;
    }

    public ConfigData impMqttUri(URI impMqttUri) {
        setImpMqttUri(impMqttUri);
        return this;
    }

    public ConfigData deviceID(String deviceID) {
        setDeviceID(deviceID);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ConfigData)) {
            return false;
        }
        ConfigData configData = (ConfigData) o;
        return Objects.equals(configFilePath, configData.configFilePath)
                && Objects.equals(caCertPath, configData.caCertPath)
                && Objects.equals(clientCertPath, configData.clientCertPath)
                && Objects.equals(keyFilePath, configData.keyFilePath)
                && Objects.equals(impVendor, configData.impVendor)
                && Objects.equals(networkType, configData.networkType)
                && Objects.equals(impMqttUri, configData.impMqttUri) && Objects.equals(deviceID, configData.deviceID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configFilePath, caCertPath, clientCertPath, keyFilePath, impVendor, networkType, impMqttUri,
                deviceID);
    }

    @Override
    public String toString() {
        return "{" + " configFilePath='" + getConfigFilePath() + "'" + ", caCertPath='" + getCaCertPath() + "'"
                + ", clientCertPath='" + getClientCertPath() + "'" + ", keyFilePath='" + getKeyFilePath() + "'"
                + ", impVendor='" + getImpVendor() + "'" + ", networkType='" + getNetworkType() + "'" + ", impMqttUri='"
                + getImpMqttUri() + "'" + ", deviceID='" + getDeviceID() + "'" + "}";
    }

}