
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "itis" })
public class Item implements Serializable {

    @JsonProperty("itis")
    private String itis;
    private final static long serialVersionUID = -6349382830896258715L;

    @JsonProperty("itis")
    public String getItis() {
        return itis;
    }

    @JsonProperty("itis")
    public void setItis(String itis) {
        this.itis = itis;
    }

}
