
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "delta" })
public class NodeLL implements Serializable {

    @JsonProperty("delta")
    private Delta delta;
    private final static long serialVersionUID = -4961941043994003136L;

    @JsonProperty("delta")
    public Delta getDelta() {
        return delta;
    }

    @JsonProperty("delta")
    public void setDelta(Delta delta) {
        this.delta = delta;
    }

}
