package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapSchedulerAdaptorDescription;
import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;

import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import org.junit.Test;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.SchedulerAdaptorDescription;

import java.util.List;


public class MapSchedulerAdaptorDescriptionTest {

    /**
     * Description which has no values the same as the proto default
     */
    class MockDescription implements SchedulerAdaptorDescription {

        @Override
        public boolean isEmbedded() {
            return true;
        }

        @Override
        public boolean supportsBatch() {
            return true;
        }

        @Override
        public boolean supportsInteractive() {
            return true;
        }

        @Override
        public boolean usesFileSystem() {
            return true;
        }

        @Override
        public String getName() {
            return "mock";
        }

        @Override
        public String getDescription() {
            return "Some description";
        }

        @Override
        public String[] getSupportedLocations() {
            return new String[]{"host"};
        }

        @Override
        public Class[] getSupportedCredentials() {
            return new Class[]{DefaultCredential.class};
        }

        @Override
        public XenonPropertyDescription[] getSupportedProperties() {
            return new XenonPropertyDescription[]{
                    new XenonPropertyDescription("propname", XenonPropertyDescription.Type.INTEGER, "42", "Mock prop desc")
            };
        }
    }

    @Test
    public void test_withMockedDescription() {
        SchedulerAdaptorDescription xenon_description = new MockDescription();

        XenonProto.SchedulerAdaptorDescription grpc_description = mapSchedulerAdaptorDescription(xenon_description);

        assertTrue("is embedded", grpc_description.getIsEmbedded());
        assertTrue("supports batch", grpc_description.getSupportsBatch());
        assertTrue("supports interactive", grpc_description.getSupportsInteractive());
        assertTrue("uses fs", grpc_description.getUsesFileSystem());
        assertEquals("mock", grpc_description.getName());
        assertEquals("Some description", grpc_description.getDescription());
        assertArrayEquals(new String[]{"host"}, grpc_description.getSupportedLocationsList().toArray());
        assertArrayEquals(new String[]{"DefaultCredential"}, grpc_description.getSupportedCredentialsList().toArray());
        List<XenonProto.PropertyDescription> expectedPropDesc = XenonProto.PropertyDescriptions.newBuilder().addProperties(
                XenonProto.PropertyDescription.newBuilder()
                        .setName("propname")
                        .setType(XenonProto.PropertyDescription.Type.INTEGER)
                        .setDefaultValue("42")
                        .setDescription("Mock prop desc")
        ).build().getPropertiesList();
        assertEquals(expectedPropDesc, grpc_description.getSupportedPropertiesList());
    }
}
