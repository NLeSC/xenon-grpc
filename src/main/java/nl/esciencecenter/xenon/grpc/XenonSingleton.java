package nl.esciencecenter.xenon.grpc;

import java.util.HashMap;
import java.util.Map;

import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;

import io.grpc.StatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
}
