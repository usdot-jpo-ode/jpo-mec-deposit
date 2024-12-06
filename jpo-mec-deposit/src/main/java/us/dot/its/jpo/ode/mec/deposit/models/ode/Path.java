
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "offset", "scale" })
public class Path implements Serializable {

    @JsonProperty("offset")
    private Offset offset;
    @JsonProperty("scale")
    private String scale;
    private final static long serialVersionUID = -4476195876171425290L;

    @JsonProperty("offset")
    public Offset getOffset() {
        return offset;
    }

    @JsonProperty("offset")
    public void setOffset(Offset offset) {
        this.offset = offset;
    }

    @JsonProperty("scale")
    public String getScale() {
        return scale;
    }

    @JsonProperty("scale")
    public void setScale(String scale) {
        this.scale = scale;
    }

}
