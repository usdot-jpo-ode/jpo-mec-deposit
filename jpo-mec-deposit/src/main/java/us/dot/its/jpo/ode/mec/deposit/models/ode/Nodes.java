
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "NodeLL" })
public class Nodes implements Serializable {

    @JsonProperty("NodeLL")
    private List<NodeLL> nodeLL;
    private final static long serialVersionUID = -6284568819422374802L;

    @JsonProperty("NodeLL")
    public List<NodeLL> getNodeLL() {
        return nodeLL;
    }

    @JsonProperty("NodeLL")
    public void setNodeLL(List<NodeLL> nodeLL) {
        this.nodeLL = nodeLL;
    }

}
