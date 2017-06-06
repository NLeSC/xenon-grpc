package nl.esciencecenter.xenon.grpc;

import io.grpc.StatusException;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class XenonSingleton {
    private Logger logger = LoggerFactory.getLogger(XenonSingleton.class);
    private Xenon instance = null;
    private Map<String,String> properties = new HashMap<>();

    public Xenon getInstance() {
        if (instance == null) {
            try {
                instance = XenonFactory.newXenon(properties);
            } catch (XenonException e) {
                // TODO something better
                logger.error(e.getMessage(), e);
            }
        }
        return instance;
    }

    void setProperties(Map<String, String> properties) throws StatusException {
        if (instance != null) {
            throw io.grpc.Status.FAILED_PRECONDITION.withDescription("Unable to set properties after other calls").asException();
        }
        this.properties = properties;
    }

    public void close() {
        if (instance != null) {
            try {
                XenonFactory.endXenon(instance);
            } catch (XenonException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
