
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "advisory" })
public class Content implements Serializable {

    @JsonProperty("advisory")
    private Advisory advisory;
    private final static long serialVersionUID = -7942632653735936719L;

    @JsonProperty("advisory")
    public Advisory getAdvisory() {
        return advisory;
    }

    @JsonProperty("advisory")
    public void setAdvisory(Advisory advisory) {
        this.advisory = advisory;
    }

}
