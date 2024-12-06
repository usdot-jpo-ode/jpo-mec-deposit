
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "rsus" })
public class Rsus implements Serializable {

    @JsonProperty("rsus")
    private Rsus__1 rsus;
    private final static long serialVersionUID = -7066039780051165552L;

    @JsonProperty("rsus")
    public Rsus__1 getRsus() {
        return rsus;
    }

    @JsonProperty("rsus")
    public void setRsus(Rsus__1 rsus) {
        this.rsus = rsus;
    }

}
