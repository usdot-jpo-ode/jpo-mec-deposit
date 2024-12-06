
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "path" })
public class Description implements Serializable {

    @JsonProperty("path")
    private Path path;
    private final static long serialVersionUID = -8048826246903758870L;

    @JsonProperty("path")
    public Path getPath() {
        return path;
    }

    @JsonProperty("path")
    public void setPath(Path path) {
        this.path = path;
    }

}
