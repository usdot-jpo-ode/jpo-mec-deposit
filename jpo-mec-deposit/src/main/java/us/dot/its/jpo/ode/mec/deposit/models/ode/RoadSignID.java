
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "viewAngle", "mutcdCode", "position" })
public class RoadSignID implements Serializable {

    @JsonProperty("viewAngle")
    private String viewAngle;
    @JsonProperty("mutcdCode")
    private MutcdCode mutcdCode;
    @JsonProperty("position")
    private Position position;
    private final static long serialVersionUID = -85155399801076666L;

    @JsonProperty("viewAngle")
    public String getViewAngle() {
        return viewAngle;
    }

    @JsonProperty("viewAngle")
    public void setViewAngle(String viewAngle) {
        this.viewAngle = viewAngle;
    }

    @JsonProperty("mutcdCode")
    public MutcdCode getMutcdCode() {
        return mutcdCode;
    }

    @JsonProperty("mutcdCode")
    public void setMutcdCode(MutcdCode mutcdCode) {
        this.mutcdCode = mutcdCode;
    }

    @JsonProperty("position")
    public Position getPosition() {
        return position;
    }

    @JsonProperty("position")
    public void setPosition(Position position) {
        this.position = position;
    }

}
