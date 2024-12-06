
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "verb", "version" })
public class Ode implements Serializable {

    @JsonProperty("verb")
    private String verb;
    @JsonProperty("version")
    private String version;
    private final static long serialVersionUID = -4673571462017050524L;

    @JsonProperty("verb")
    public String getVerb() {
        return verb;
    }

    @JsonProperty("verb")
    public void setVerb(String verb) {
        this.verb = verb;
    }

    @JsonProperty("version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(String version) {
        this.version = version;
    }

}
