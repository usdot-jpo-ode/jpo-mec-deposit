
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "TravelerDataFrame" })
public class DataFrames implements Serializable {

    @JsonProperty("TravelerDataFrame")
    private TravelerDataFrame travelerDataFrame;
    private final static long serialVersionUID = -6690045690103071914L;

    @JsonProperty("TravelerDataFrame")
    public TravelerDataFrame getTravelerDataFrame() {
        return travelerDataFrame;
    }

    @JsonProperty("TravelerDataFrame")
    public void setTravelerDataFrame(TravelerDataFrame travelerDataFrame) {
        this.travelerDataFrame = travelerDataFrame;
    }

}
