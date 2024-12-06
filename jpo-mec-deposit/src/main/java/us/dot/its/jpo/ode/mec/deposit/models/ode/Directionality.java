
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "both" })
public class Directionality implements Serializable {

    @JsonProperty("both")
    private String both;
    private final static long serialVersionUID = 5673948683842479769L;

    @JsonProperty("both")
    public String getBoth() {
        return both;
    }

    @JsonProperty("both")
    public void setBoth(String both) {
        this.both = both;
    }

}
