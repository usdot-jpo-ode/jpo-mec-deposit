package us.dot.its.jpo.ode.mec.deposit.serialization;

import us.dot.its.jpo.ode.mec.deposit.serialization.deserializers.*;
import us.dot.its.jpo.ode.mec.deposit.serialization.serializers.*;

import us.dot.its.jpo.ode.model.OdeMapData;
import us.dot.its.jpo.ode.model.OdeSpatData;
import us.dot.its.jpo.ode.model.OdeBsmData;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Contains static methods that return a "Serde", a serializer/deserializer for
 * JSON for Kafka, for each POJO type.
 */
public class JsonSerdes {
    public static Serde<OdeMapData> OdeMap() {
        return Serdes.serdeFrom(new JsonSerializer<OdeMapData>(), new JsonDeserializer<>(OdeMapData.class));
    }

    public static Serde<OdeSpatData> OdeSpat() {
        return Serdes.serdeFrom(new JsonSerializer<OdeSpatData>(), new JsonDeserializer<>(OdeSpatData.class));
    }

    public static Serde<OdeBsmData> OdeBsm() {
        return Serdes.serdeFrom(new JsonSerializer<OdeBsmData>(), new JsonDeserializer<>(OdeBsmData.class));
    }

}
