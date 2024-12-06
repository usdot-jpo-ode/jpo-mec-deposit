
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "SEQUENCE" })
public class Advisory implements Serializable {

    @JsonProperty("SEQUENCE")
    private Sequence sequence;
    private final static long serialVersionUID = 4707340255249924605L;

    @JsonProperty("SEQUENCE")
    public Sequence getSequence() {
        return sequence;
    }

    @JsonProperty("SEQUENCE")
    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

}
