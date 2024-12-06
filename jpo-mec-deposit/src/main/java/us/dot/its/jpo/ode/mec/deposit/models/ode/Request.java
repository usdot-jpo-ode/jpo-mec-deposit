
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "ode", "rsus", "snmp" })
public class Request implements Serializable {

    @JsonProperty("ode")
    private Ode ode;
    @JsonProperty("rsus")
    private Rsus rsus;
    @JsonProperty("snmp")
    private Snmp snmp;
    private final static long serialVersionUID = 1466257249060247550L;

    @JsonProperty("ode")
    public Ode getOde() {
        return ode;
    }

    @JsonProperty("ode")
    public void setOde(Ode ode) {
        this.ode = ode;
    }

    @JsonProperty("rsus")
    public Rsus getRsus() {
        return rsus;
    }

    @JsonProperty("rsus")
    public void setRsus(Rsus rsus) {
        this.rsus = rsus;
    }

    @JsonProperty("snmp")
    public Snmp getSnmp() {
        return snmp;
    }

    @JsonProperty("snmp")
    public void setSnmp(Snmp snmp) {
        this.snmp = snmp;
    }

}
