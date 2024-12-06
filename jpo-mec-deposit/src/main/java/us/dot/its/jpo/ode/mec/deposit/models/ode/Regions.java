
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "GeographicalPath" })
public class Regions implements Serializable {

    @JsonProperty("GeographicalPath")
    private GeographicalPath geographicalPath;
    private final static long serialVersionUID = -2244359660586010284L;

    @JsonProperty("GeographicalPath")
    public GeographicalPath getGeographicalPath() {
        return geographicalPath;
    }

    @JsonProperty("GeographicalPath")
    public void setGeographicalPath(GeographicalPath geographicalPath) {
        this.geographicalPath = geographicalPath;
    }

}
