
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "roadSignID" })
public class MsgId implements Serializable {

    @JsonProperty("roadSignID")
    private RoadSignID roadSignID;
    private final static long serialVersionUID = 23319067586152473L;

    @JsonProperty("roadSignID")
    public RoadSignID getRoadSignID() {
        return roadSignID;
    }

    @JsonProperty("roadSignID")
    public void setRoadSignID(RoadSignID roadSignID) {
        this.roadSignID = roadSignID;
    }

}
