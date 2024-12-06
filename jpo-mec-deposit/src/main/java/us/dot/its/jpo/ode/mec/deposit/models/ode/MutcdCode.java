
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "warning" })
public class MutcdCode implements Serializable {

    @JsonProperty("warning")
    private String warning;
    private final static long serialVersionUID = 6846288086449775173L;

    @JsonProperty("warning")
    public String getWarning() {
        return warning;
    }

    @JsonProperty("warning")
    public void setWarning(String warning) {
        this.warning = warning;
    }

}
