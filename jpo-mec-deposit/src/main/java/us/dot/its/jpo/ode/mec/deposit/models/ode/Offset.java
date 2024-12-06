
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "ll" })
public class Offset implements Serializable {

    @JsonProperty("ll")
    private Ll ll;
    private final static long serialVersionUID = 2938035399765439565L;

    @JsonProperty("ll")
    public Ll getLl() {
        return ll;
    }

    @JsonProperty("ll")
    public void setLl(Ll ll) {
        this.ll = ll;
    }

}
