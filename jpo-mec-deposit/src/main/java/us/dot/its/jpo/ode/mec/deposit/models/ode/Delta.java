
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "node-LL2", "node-LL1", "node-LL3", "node-LL4", "node-LL5", "node-LL6" })
public class Delta implements Serializable {

    @JsonProperty("node-LL2")
    private NodeLLGeneric nodeLL2;
    @JsonProperty("node-LL1")
    private NodeLLGeneric nodeLL1;
    private final static long serialVersionUID = 6901929116021526241L;

    @JsonProperty("node-LL2")
    public NodeLLGeneric getNodeLL2() {
        return nodeLL2;
    }

    @JsonProperty("node-LL2")
    public void setNodeLL2(NodeLLGeneric nodeLL2) {
        this.nodeLL2 = nodeLL2;
    }

    @JsonProperty("node-LL1")
    public NodeLLGeneric getNodeLL1() {
        return nodeLL1;
    }

    @JsonProperty("node-LL1")
    public void setNodeLL1(NodeLLGeneric nodeLL1) {
        this.nodeLL1 = nodeLL1;
    }

    @JsonProperty("node-LL3")

    private NodeLLGeneric nodeLL3;

    @JsonProperty("node-LL4")
    private NodeLLGeneric nodeLL4;

    @JsonProperty("node-LL5")
    private NodeLLGeneric nodeLL5;

    @JsonProperty("node-LL6")
    private NodeLLGeneric nodeLL6;

    @JsonProperty("node-LL3")
    public NodeLLGeneric getNodeLL3() {
        return nodeLL3;
    }

    @JsonProperty("node-LL3")
    public void setNodeLL3(NodeLLGeneric nodeLL3) {
        this.nodeLL3 = nodeLL3;
    }

    @JsonProperty("node-LL4")
    public NodeLLGeneric getNodeLL4() {
        return nodeLL4;
    }

    @JsonProperty("node-LL4")
    public void setNodeLL4(NodeLLGeneric nodeLL4) {
        this.nodeLL4 = nodeLL4;
    }

    @JsonProperty("node-LL5")
    public NodeLLGeneric getNodeLL5() {
        return nodeLL5;
    }

    @JsonProperty("node-LL5")
    public void setNodeLL5(NodeLLGeneric nodeLL5) {
        this.nodeLL5 = nodeLL5;
    }

    @JsonProperty("node-LL6")
    public NodeLLGeneric getNodeLL6() {
        return nodeLL6;
    }

    @JsonProperty("node-LL6")
    public void setNodeLL6(NodeLLGeneric nodeLL6) {
        this.nodeLL6 = nodeLL6;
    }

}
