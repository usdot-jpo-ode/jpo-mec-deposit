
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "rsuTarget", "rsuUsername", "snmpProtocol", "rsuRetries", "rsuTimeout", "rsuPassword",
        "rsuIndex" })
public class Rsus__1 implements Serializable {

    @JsonProperty("rsuTarget")
    private String rsuTarget;
    @JsonProperty("rsuUsername")
    private String rsuUsername;
    @JsonProperty("snmpProtocol")
    private String snmpProtocol;
    @JsonProperty("rsuRetries")
    private String rsuRetries;
    @JsonProperty("rsuTimeout")
    private String rsuTimeout;
    @JsonProperty("rsuPassword")
    private String rsuPassword;
    @JsonProperty("rsuIndex")
    private String rsuIndex;
    private final static long serialVersionUID = -4706021266580631019L;

    @JsonProperty("rsuTarget")
    public String getRsuTarget() {
        return rsuTarget;
    }

    @JsonProperty("rsuTarget")
    public void setRsuTarget(String rsuTarget) {
        this.rsuTarget = rsuTarget;
    }

    @JsonProperty("rsuUsername")
    public String getRsuUsername() {
        return rsuUsername;
    }

    @JsonProperty("rsuUsername")
    public void setRsuUsername(String rsuUsername) {
        this.rsuUsername = rsuUsername;
    }

    @JsonProperty("snmpProtocol")
    public String getSnmpProtocol() {
        return snmpProtocol;
    }

    @JsonProperty("snmpProtocol")
    public void setSnmpProtocol(String snmpProtocol) {
        this.snmpProtocol = snmpProtocol;
    }

    @JsonProperty("rsuRetries")
    public String getRsuRetries() {
        return rsuRetries;
    }

    @JsonProperty("rsuRetries")
    public void setRsuRetries(String rsuRetries) {
        this.rsuRetries = rsuRetries;
    }

    @JsonProperty("rsuTimeout")
    public String getRsuTimeout() {
        return rsuTimeout;
    }

    @JsonProperty("rsuTimeout")
    public void setRsuTimeout(String rsuTimeout) {
        this.rsuTimeout = rsuTimeout;
    }

    @JsonProperty("rsuPassword")
    public String getRsuPassword() {
        return rsuPassword;
    }

    @JsonProperty("rsuPassword")
    public void setRsuPassword(String rsuPassword) {
        this.rsuPassword = rsuPassword;
    }

    @JsonProperty("rsuIndex")
    public String getRsuIndex() {
        return rsuIndex;
    }

    @JsonProperty("rsuIndex")
    public void setRsuIndex(String rsuIndex) {
        this.rsuIndex = rsuIndex;
    }

}
