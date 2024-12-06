
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "advisory" })
public class FrameType implements Serializable {

    @JsonProperty("advisory")
    private String advisory;
    private final static long serialVersionUID = -3815631988620379113L;

    @JsonProperty("advisory")
    public String getAdvisory() {
        return advisory;
    }

    @JsonProperty("advisory")
    public void setAdvisory(String advisory) {
        this.advisory = advisory;
    }

}
