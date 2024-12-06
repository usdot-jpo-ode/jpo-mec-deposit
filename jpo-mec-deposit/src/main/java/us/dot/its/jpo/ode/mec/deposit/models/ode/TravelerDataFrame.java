
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "durationTime", "regions", "startYear", "notUsed2", "msgId", "notUsed3", "notUsed1", "priority",
        "content", "url", "notUsed", "frameType", "startTime" })
public class TravelerDataFrame implements Serializable {

    @JsonProperty("durationTime")
    private String durationTime;
    @JsonProperty("regions")
    private Regions regions;
    @JsonProperty("startYear")
    private String startYear;
    @JsonProperty("notUsed2")
    private String notUsed2;
    @JsonProperty("msgId")
    private MsgId msgId;
    @JsonProperty("notUsed3")
    private String notUsed3;
    @JsonProperty("notUsed1")
    private String notUsed1;
    @JsonProperty("priority")
    private String priority;
    @JsonProperty("content")
    private Content content;
    @JsonProperty("url")
    private String url;
    @JsonProperty("notUsed")
    private String notUsed;
    @JsonProperty("frameType")
    private FrameType frameType;
    @JsonProperty("startTime")
    private String startTime;
    private final static long serialVersionUID = 3504311727000293273L;

    @JsonProperty("durationTime")
    public String getDurationTime() {
        return durationTime;
    }

    @JsonProperty("durationTime")
    public void setDurationTime(String durationTime) {
        this.durationTime = durationTime;
    }

    @JsonProperty("regions")
    public Regions getRegions() {
        return regions;
    }

    @JsonProperty("regions")
    public void setRegions(Regions regions) {
        this.regions = regions;
    }

    @JsonProperty("startYear")
    public String getStartYear() {
        return startYear;
    }

    @JsonProperty("startYear")
    public void setStartYear(String startYear) {
        this.startYear = startYear;
    }

    @JsonProperty("notUsed2")
    public String getNotUsed2() {
        return notUsed2;
    }

    @JsonProperty("notUsed2")
    public void setNotUsed2(String notUsed2) {
        this.notUsed2 = notUsed2;
    }

    @JsonProperty("msgId")
    public MsgId getMsgId() {
        return msgId;
    }

    @JsonProperty("msgId")
    public void setMsgId(MsgId msgId) {
        this.msgId = msgId;
    }

    @JsonProperty("notUsed3")
    public String getNotUsed3() {
        return notUsed3;
    }

    @JsonProperty("notUsed3")
    public void setNotUsed3(String notUsed3) {
        this.notUsed3 = notUsed3;
    }

    @JsonProperty("notUsed1")
    public String getNotUsed1() {
        return notUsed1;
    }

    @JsonProperty("notUsed1")
    public void setNotUsed1(String notUsed1) {
        this.notUsed1 = notUsed1;
    }

    @JsonProperty("priority")
    public String getPriority() {
        return priority;
    }

    @JsonProperty("priority")
    public void setPriority(String priority) {
        this.priority = priority;
    }

    @JsonProperty("content")
    public Content getContent() {
        return content;
    }

    @JsonProperty("content")
    public void setContent(Content content) {
        this.content = content;
    }

    @JsonProperty("url")
    public String getUrl() {
        return url;
    }

    @JsonProperty("url")
    public void setUrl(String url) {
        this.url = url;
    }

    @JsonProperty("notUsed")
    public String getNotUsed() {
        return notUsed;
    }

    @JsonProperty("notUsed")
    public void setNotUsed(String notUsed) {
        this.notUsed = notUsed;
    }

    @JsonProperty("frameType")
    public FrameType getFrameType() {
        return frameType;
    }

    @JsonProperty("frameType")
    public void setFrameType(FrameType frameType) {
        this.frameType = frameType;
    }

    @JsonProperty("startTime")
    public String getStartTime() {
        return startTime;
    }

    @JsonProperty("startTime")
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

}
