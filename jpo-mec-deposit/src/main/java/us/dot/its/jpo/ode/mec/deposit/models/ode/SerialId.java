
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "recordId", "serialNumber", "streamId", "bundleSize", "bundleId" })
public class SerialId implements Serializable {

    @JsonProperty("recordId")
    private String recordId;
    @JsonProperty("serialNumber")
    private String serialNumber;
    @JsonProperty("streamId")
    private String streamId;
    @JsonProperty("bundleSize")
    private String bundleSize;
    @JsonProperty("bundleId")
    private String bundleId;
    private final static long serialVersionUID = -1594653191405346199L;

    @JsonProperty("recordId")
    public String getRecordId() {
        return recordId;
    }

    @JsonProperty("recordId")
    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    @JsonProperty("serialNumber")
    public String getSerialNumber() {
        return serialNumber;
    }

    @JsonProperty("serialNumber")
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
    }

    @JsonProperty("streamId")
    public String getStreamId() {
        return streamId;
    }

    @JsonProperty("streamId")
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    @JsonProperty("bundleSize")
    public String getBundleSize() {
        return bundleSize;
    }

    @JsonProperty("bundleSize")
    public void setBundleSize(String bundleSize) {
        this.bundleSize = bundleSize;
    }

    @JsonProperty("bundleId")
    public String getBundleId() {
        return bundleId;
    }

    @JsonProperty("bundleId")
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

}
