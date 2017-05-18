package nl.esciencecenter.xenon.grpc;

import java.util.HashMap;
import java.util.Map;

import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XenonSingleton {
    Logger logger = LoggerFactory.getLogger(XenonSingleton.class);
    private Xenon instance = null;
    private Map<String,String> properties = new HashMap<>();

    public Xenon getInstance() {
        if (instance == null) {
            try {
                instance = XenonFactory.newXenon(properties);
            } catch (XenonException e) {
                // TODO something better
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
        return instance;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
        // TODO complain when instance already exists with different properties
    }
}
