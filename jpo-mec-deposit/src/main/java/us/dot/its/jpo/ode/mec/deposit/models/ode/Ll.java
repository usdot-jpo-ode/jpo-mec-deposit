
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "nodes" })
public class Ll implements Serializable {

    @JsonProperty("nodes")
    private Nodes nodes;
    private final static long serialVersionUID = -3639785425795846668L;

    @JsonProperty("nodes")
    public Nodes getNodes() {
        return nodes;
    }

    @JsonProperty("nodes")
    public void setNodes(Nodes nodes) {
        this.nodes = nodes;
    }

}
