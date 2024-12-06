
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "TravelerInformation" })
public class Value implements Serializable {

    @JsonProperty("TravelerInformation")
    private TravelerInformation travelerInformation;
    private final static long serialVersionUID = 1512301206382722514L;

    @JsonProperty("TravelerInformation")
    public TravelerInformation getTravelerInformation() {
        return travelerInformation;
    }

    @JsonProperty("TravelerInformation")
    public void setTravelerInformation(TravelerInformation travelerInformation) {
        this.travelerInformation = travelerInformation;
    }

}
