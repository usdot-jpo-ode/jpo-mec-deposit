
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "MessageFrame" })
public class Data implements Serializable {

    @JsonProperty("MessageFrame")
    private MessageFrame messageFrame;
    private final static long serialVersionUID = 8467424776321501682L;

    @JsonProperty("MessageFrame")
    public MessageFrame getMessageFrame() {
        return messageFrame;
    }

    @JsonProperty("MessageFrame")
    public void setMessageFrame(MessageFrame messageFrame) {
        this.messageFrame = messageFrame;
    }

}
