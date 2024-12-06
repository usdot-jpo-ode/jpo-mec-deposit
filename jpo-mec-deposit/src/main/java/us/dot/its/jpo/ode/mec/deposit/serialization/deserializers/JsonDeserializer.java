package us.dot.its.jpo.ode.mec.deposit.serialization.deserializers;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.ode.mec.deposit.DateJsonMapper;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    protected final ObjectMapper mapper = DateJsonMapper.getInstance();

    private Class<T> destinationClass;

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            T returnData = mapper.readValue(data, destinationClass);
            return returnData;
        } catch (IOException e) {
            String errMsg = String.format("Exception deserializing for topic %s: %s", topic, e.getMessage());
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }
}
