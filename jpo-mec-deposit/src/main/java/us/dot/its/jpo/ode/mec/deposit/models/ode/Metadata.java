
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "request", "recordGeneratedBy", "schemaVersion", "payloadType", "odePacketID", "serialId",
        "sanitized", "recordGeneratedAt", "asn1", "maxDurationTime", "odeTimStartDateTime", "odeReceivedAt" })
public class Metadata implements Serializable {

    @JsonProperty("request")
    private Request request;
    @JsonProperty("recordGeneratedBy")
    private String recordGeneratedBy;
    @JsonProperty("schemaVersion")
    private String schemaVersion;
    @JsonProperty("payloadType")
    private String payloadType;
    @JsonProperty("odePacketID")
    private String odePacketID;
    @JsonProperty("serialId")
    private SerialId serialId;
    @JsonProperty("sanitized")
    private String sanitized;
    @JsonProperty("recordGeneratedAt")
    private String recordGeneratedAt;
    @JsonProperty("asn1")
    private String asn1;
    @JsonProperty("maxDurationTime")
    private String maxDurationTime;
    @JsonProperty("odeTimStartDateTime")
    private String odeTimStartDateTime;
    @JsonProperty("odeReceivedAt")
    private String odeReceivedAt;
    private final static long serialVersionUID = -2789868510926031841L;

    @JsonProperty("request")
    public Request getRequest() {
        return request;
    }

    @JsonProperty("request")
    public void setRequest(Request request) {
        this.request = request;
    }

    @JsonProperty("recordGeneratedBy")
    public String getRecordGeneratedBy() {
        return recordGeneratedBy;
    }

    @JsonProperty("recordGeneratedBy")
    public void setRecordGeneratedBy(String recordGeneratedBy) {
        this.recordGeneratedBy = recordGeneratedBy;
    }

    @JsonProperty("schemaVersion")
    public String getSchemaVersion() {
        return schemaVersion;
    }

    @JsonProperty("schemaVersion")
    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    @JsonProperty("payloadType")
    public String getPayloadType() {
        return payloadType;
    }

    @JsonProperty("payloadType")
    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    @JsonProperty("odePacketID")
    public String getOdePacketID() {
        return odePacketID;
    }

    @JsonProperty("odePacketID")
    public void setOdePacketID(String odePacketID) {
        this.odePacketID = odePacketID;
    }

    @JsonProperty("serialId")
    public SerialId getSerialId() {
        return serialId;
    }

    @JsonProperty("serialId")
    public void setSerialId(SerialId serialId) {
        this.serialId = serialId;
    }

    @JsonProperty("sanitized")
    public String getSanitized() {
        return sanitized;
    }

    @JsonProperty("sanitized")
    public void setSanitized(String sanitized) {
        this.sanitized = sanitized;
    }

    @JsonProperty("recordGeneratedAt")
    public String getRecordGeneratedAt() {
        return recordGeneratedAt;
    }

    @JsonProperty("recordGeneratedAt")
    public void setRecordGeneratedAt(String recordGeneratedAt) {
        this.recordGeneratedAt = recordGeneratedAt;
    }

    @JsonProperty("asn1")
    public String getAsn1() {
        return asn1;
    }

    @JsonProperty("asn1")
    public void setAsn1(String asn1) {
        this.asn1 = asn1;
    }

    @JsonProperty("maxDurationTime")
    public String getMaxDurationTime() {
        return maxDurationTime;
    }

    @JsonProperty("maxDurationTime")
    public void setMaxDurationTime(String maxDurationTime) {
        this.maxDurationTime = maxDurationTime;
    }

    @JsonProperty("odeTimStartDateTime")
    public String getOdeTimStartDateTime() {
        return odeTimStartDateTime;
    }

    @JsonProperty("odeTimStartDateTime")
    public void setOdeTimStartDateTime(String odeTimStartDateTime) {
        this.odeTimStartDateTime = odeTimStartDateTime;
    }

    @JsonProperty("odeReceivedAt")
    public String getOdeReceivedAt() {
        return odeReceivedAt;
    }

    @JsonProperty("odeReceivedAt")
    public void setOdeReceivedAt(String odeReceivedAt) {
        this.odeReceivedAt = odeReceivedAt;
    }

}
