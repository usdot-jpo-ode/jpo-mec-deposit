
package us.dot.its.jpo.ode.mec.deposit.models.ode;

import java.io.Serializable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "closedPath", "anchor", "name", "laneWidth", "directionality", "description", "id", "direction" })
public class GeographicalPath implements Serializable {

    @JsonProperty("closedPath")
    private ClosedPath closedPath;
    @JsonProperty("anchor")
    private Anchor anchor;
    @JsonProperty("name")
    private String name;
    @JsonProperty("laneWidth")
    private String laneWidth;
    @JsonProperty("directionality")
    private Directionality directionality;
    @JsonProperty("description")
    private Description description;
    @JsonProperty("id")
    private Id id;
    @JsonProperty("direction")
    private String direction;
    private final static long serialVersionUID = -2461275154775010034L;

    @JsonProperty("closedPath")
    public ClosedPath getClosedPath() {
        return closedPath;
    }

    @JsonProperty("closedPath")
    public void setClosedPath(ClosedPath closedPath) {
        this.closedPath = closedPath;
    }

    @JsonProperty("anchor")
    public Anchor getAnchor() {
        return anchor;
    }

    @JsonProperty("anchor")
    public void setAnchor(Anchor anchor) {
        this.anchor = anchor;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("laneWidth")
    public String getLaneWidth() {
        return laneWidth;
    }

    @JsonProperty("laneWidth")
    public void setLaneWidth(String laneWidth) {
        this.laneWidth = laneWidth;
    }

    @JsonProperty("directionality")
    public Directionality getDirectionality() {
        return directionality;
    }

    @JsonProperty("directionality")
    public void setDirectionality(Directionality directionality) {
        this.directionality = directionality;
    }

    @JsonProperty("description")
    public Description getDescription() {
        return description;
    }

    @JsonProperty("description")
    public void setDescription(Description description) {
        this.description = description;
    }

    @JsonProperty("id")
    public Id getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(Id id) {
        this.id = id;
    }

    @JsonProperty("direction")
    public String getDirection() {
        return direction;
    }

    @JsonProperty("direction")
    public void setDirection(String direction) {
        this.direction = direction;
    }

}
