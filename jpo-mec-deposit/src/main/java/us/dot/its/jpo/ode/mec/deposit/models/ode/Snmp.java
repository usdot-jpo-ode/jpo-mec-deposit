
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "mode", "deliverystop", "rsuid", "deliverystart", "enable", "channel", "msgid", "interval",
        "status" })
public class Snmp implements Serializable {

    @JsonProperty("mode")
    private String mode;
    @JsonProperty("deliverystop")
    private String deliverystop;
    @JsonProperty("rsuid")
    private String rsuid;
    @JsonProperty("deliverystart")
    private String deliverystart;
    @JsonProperty("enable")
    private String enable;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("msgid")
    private String msgid;
    @JsonProperty("interval")
    private String interval;
    @JsonProperty("status")
    private String status;
    private final static long serialVersionUID = -8955341682221998057L;

    @JsonProperty("mode")
    public String getMode() {
        return mode;
    }

    @JsonProperty("mode")
    public void setMode(String mode) {
        this.mode = mode;
    }

    @JsonProperty("deliverystop")
    public String getDeliverystop() {
        return deliverystop;
    }

    @JsonProperty("deliverystop")
    public void setDeliverystop(String deliverystop) {
        this.deliverystop = deliverystop;
    }

    @JsonProperty("rsuid")
    public String getRsuid() {
        return rsuid;
    }

    @JsonProperty("rsuid")
    public void setRsuid(String rsuid) {
        this.rsuid = rsuid;
    }

    @JsonProperty("deliverystart")
    public String getDeliverystart() {
        return deliverystart;
    }

    @JsonProperty("deliverystart")
    public void setDeliverystart(String deliverystart) {
        this.deliverystart = deliverystart;
    }

    @JsonProperty("enable")
    public String getEnable() {
        return enable;
    }

    @JsonProperty("enable")
    public void setEnable(String enable) {
        this.enable = enable;
    }

    @JsonProperty("channel")
    public String getChannel() {
        return channel;
    }

    @JsonProperty("channel")
    public void setChannel(String channel) {
        this.channel = channel;
    }

    @JsonProperty("msgid")
    public String getMsgid() {
        return msgid;
    }

    @JsonProperty("msgid")
    public void setMsgid(String msgid) {
        this.msgid = msgid;
    }

    @JsonProperty("interval")
    public String getInterval() {
        return interval;
    }

    @JsonProperty("interval")
    public void setInterval(String interval) {
        this.interval = interval;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

}
