
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "false" })
public class ClosedPath implements Serializable {

    @JsonProperty("false")
    private String _false;
    private final static long serialVersionUID = -3067450038121407042L;

    @JsonProperty("false")
    public String getFalse() {
        return _false;
    }

    @JsonProperty("false")
    public void setFalse(String _false) {
        this._false = _false;
    }

}
