
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "timeStamp", "packetID", "urlB", "dataFrames", "msgCnt" })
public class TravelerInformation implements Serializable {

    @JsonProperty("timeStamp")
    private String timeStamp;
    @JsonProperty("packetID")
    private String packetID;
    @JsonProperty("urlB")
    private String urlB;
    @JsonProperty("dataFrames")
    private DataFrames dataFrames;
    @JsonProperty("msgCnt")
    private String msgCnt;
    private final static long serialVersionUID = -3584273953372309191L;

    @JsonProperty("timeStamp")
    public String getTimeStamp() {
        return timeStamp;
    }

    @JsonProperty("timeStamp")
    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    @JsonProperty("packetID")
    public String getPacketID() {
        return packetID;
    }

    @JsonProperty("packetID")
    public void setPacketID(String packetID) {
        this.packetID = packetID;
    }

    @JsonProperty("urlB")
    public String getUrlB() {
        return urlB;
    }

    @JsonProperty("urlB")
    public void setUrlB(String urlB) {
        this.urlB = urlB;
    }

    @JsonProperty("dataFrames")
    public DataFrames getDataFrames() {
        return dataFrames;
    }

    @JsonProperty("dataFrames")
    public void setDataFrames(DataFrames dataFrames) {
        this.dataFrames = dataFrames;
    }

    @JsonProperty("msgCnt")
    public String getMsgCnt() {
        return msgCnt;
    }

    @JsonProperty("msgCnt")
    public void setMsgCnt(String msgCnt) {
        this.msgCnt = msgCnt;
    }

}
