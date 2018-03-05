package nl.esciencecenter.xenon.grpc;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapCredential;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;
import static nl.esciencecenter.xenon.grpc.MapUtils.toCredentialResponse;
import static nl.esciencecenter.xenon.grpc.MapUtils.usernameOfCredential;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import nl.esciencecenter.xenon.credentials.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;

public class MapUtilsTest {
    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void mapPropertyDescriptions_booltype() throws Exception {
        XenonPropertyDescription[] input = new XenonPropertyDescription[] {
            new XenonPropertyDescription("abool", XenonPropertyDescription.Type.BOOLEAN, "false", "abool desc")
        };

        List<XenonProto.PropertyDescription> result = mapPropertyDescriptions(input);

        List<XenonProto.PropertyDescription> expected = Collections.singletonList(
                XenonProto.PropertyDescription.newBuilder()
                        .setName("abool")
                        .setType(XenonProto.PropertyDescription.Type.BOOLEAN)
                        .setDefaultValue("false")
                        .setDescription("abool desc")
                        .build()
        );
        assertEquals(expected, result);
    }

    @Test
    public void test_empty() throws Exception {
        XenonProto.Empty message = empty();

        XenonProto.Empty expected = XenonProto.Empty.getDefaultInstance();
        assertEquals(expected, message);
    }

    @Test
    public void mapCredential_fileSystem_default() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .build();

        Credential result = mapCredential(request);

        DefaultCredential expected = new DefaultCredential();
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_username() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setDefaultCredential(XenonProto.DefaultCredential.newBuilder().setUsername("someone"))
                .build();

        Credential result = mapCredential(request);

        Credential expected = new DefaultCredential("someone");
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_usernamePassword() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setPasswordCredential(XenonProto.PasswordCredential.newBuilder().setUsername("someone").setPassword("mypassword"))
                .build();

        Credential result = mapCredential(request);

        Credential expected = new PasswordCredential("someone", "mypassword".toCharArray());
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_scheduler_certificate() throws Exception {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCertificateCredential(XenonProto.CertificateCredential.newBuilder()
                        .setUsername("someone")
                        .setCertfile("/home/someone/.ssh/id_rsa")
                        .setPassphrase("mypassphrase")
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CertificateCredential("someone","/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray());
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_filesystem_default() throws Exception {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
                .setAdaptor("file")
                .build();

        Credential result = mapCredential(request);

        DefaultCredential expected = new DefaultCredential();
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_minimal() {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap();
        assertEquals(expected, result);
    }

    @Test
    public void mapCredential_filesystem_keytab() {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
                .setAdaptor("hdfs")
                .setKeytabCredential(XenonProto.KeytabCredential.newBuilder()
                        .setUsername("someone")
                        .setKeytabfile("/home/someone/mycluster.keytab")
                )
                .build();

        KeytabCredential result = (KeytabCredential) mapCredential(request);

        KeytabCredential expected = new KeytabCredential("someone", "/home/someone/mycluster.keytab");
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_default_as_fallback() {
        XenonProto.DefaultCredential.Builder fallback = XenonProto.DefaultCredential.newBuilder();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setDefaultCredential(fallback)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap(new DefaultCredential());
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_usernamedefault_as_fallback() {
        XenonProto.DefaultCredential.Builder fallback = XenonProto.DefaultCredential.newBuilder()
                .setUsername("someone");
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setDefaultCredential(fallback)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap(new DefaultCredential("someone"));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_password_as_fallback() {
        XenonProto.PasswordCredential.Builder fallback = XenonProto.PasswordCredential.newBuilder()
                .setUsername("someone")
                .setPassword("mypassword");
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setPasswordCredential(fallback)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap(new PasswordCredential("someone", "mypassword".toCharArray()));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_cert_as_fallback() {
        XenonProto.CertificateCredential.Builder fallback = XenonProto.CertificateCredential.newBuilder()
                .setUsername("someone")
                .setCertfile("/home/someone/.ssh/id_rsa")
                .setPassphrase("mypassphrase");
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setCertificateCredential(fallback)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap(new CertificateCredential("someone", "/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray()));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_keytab_as_fallback() {
        XenonProto.KeytabCredential.Builder fallback = XenonProto.KeytabCredential.newBuilder()
                .setUsername("someone")
                .setKeytabfile("/home/someone/mycluster.keytab")
                ;
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setKeytabCredential(fallback)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        Credential expected = new CredentialMap(new KeytabCredential("someone", "/home/someone/mycluster.keytab"));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_default_entry() {
        XenonProto.DefaultCredential.Builder fallback = XenonProto.DefaultCredential.newBuilder()
                .setUsername("someone");
        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setDefaultCredential(
                        XenonProto.DefaultCredential.newBuilder().setUsername("someoneelse").build()
                ).build();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setDefaultCredential(fallback)
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        CredentialMap expected = new CredentialMap(new DefaultCredential("someone"));
        expected.put("somehost", new DefaultCredential("someoneelse"));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_userdefault_entry() {
        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setDefaultCredential(
                        XenonProto.DefaultCredential.newBuilder().setUsername("someone").build()
                ).build();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        CredentialMap expected = new CredentialMap();
        expected.put("somehost", new DefaultCredential("someone"));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_password_entry() {
        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setPasswordCredential(XenonProto.PasswordCredential.newBuilder()
                        .setUsername("someone")
                        .setPassword("mypassword")
                ).build();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        CredentialMap expected = new CredentialMap();
        expected.put("somehost", new PasswordCredential("someone", "mypassword".toCharArray()));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_cert_entry() {
        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setCertificateCredential(XenonProto.CertificateCredential.newBuilder()
                        .setUsername("someone")
                        .setCertfile("/home/someone/.ssh/id_rsa")
                        .setPassphrase("mypassphrase")
                ).build();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        CredentialMap expected = new CredentialMap();
        expected.put("somehost", new CertificateCredential("someone", "/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray()));
        assertEquals(expected, result);
    }

    @Test
    public void map_Credential_filesystem_map_with_keytab_entry() {
        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setKeytabCredential(XenonProto.KeytabCredential.newBuilder()
                        .setUsername("someone")
                        .setKeytabfile("/home/someone/mycluster.keytab")
                ).build();
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("file")
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();

        Credential result = mapCredential(request);

        CredentialMap expected = new CredentialMap();
        expected.put("somehost", new KeytabCredential("someone", "/home/someone/mycluster.keytab"));
        assertEquals(expected, result);
    }

    @Test
    public void usernameOfCredential_usercredential() {
        UserCredential cred = new DefaultCredential("myusername");

        String username = usernameOfCredential(cred);

        assertEquals("myusername", username);
    }

    @Test
    public void usernameOfCredential_nonusercredential() {
        Credential cred = new CredentialMap();

        String username = usernameOfCredential(cred);

        assertEquals("nousername", username);
    }

    @Test
    public void toCredentialResponse_unknownclass_exception() throws XenonException {
        thrown.expect(XenonException.class);
        thrown.expectMessage("Unknown credential class");

        Credential cred = getUnknownCredential();

        toCredentialResponse(cred);
    }

    @Test
    public void toCredentialResponse_default() throws XenonException {
        Credential cred = new DefaultCredential("someone");

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setDefaultCredential(
                XenonProto.DefaultCredential.newBuilder().setUsername("someone")
            ).build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_password() throws XenonException {
        Credential cred = new PasswordCredential("someone", "mypassword".toCharArray());

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setPasswordCredential(
                XenonProto.PasswordCredential.newBuilder()
                    .setUsername("someone")
                    .setPassword("mypassword"))
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_certificate() throws XenonException {
        Credential cred = new CertificateCredential("someone","/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray());

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCertificateCredential(XenonProto.CertificateCredential.newBuilder()
                .setUsername("someone")
                .setCertfile("/home/someone/.ssh/id_rsa")
                .setPassphrase("mypassphrase")
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_keytab() throws XenonException {
        Credential cred = new KeytabCredential("someone","/home/someone/mycluster.keytab");

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
                .setKeytabCredential(XenonProto.KeytabCredential.newBuilder()
                        .setUsername("someone")
                        .setKeytabfile("/home/someone/mycluster.keytab")
                )
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_emptyAndNoDefault() throws XenonException {
        Credential cred = new CredentialMap();

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.getDefaultInstance())
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_defaultCredAsDefault() throws XenonException {
        Credential cred = new CredentialMap(new DefaultCredential("someone"));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .setDefaultCredential(XenonProto.DefaultCredential.newBuilder()
                    .setUsername("someone")
                )
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_passwordAsDefault() throws XenonException {
        Credential cred = new CredentialMap(new PasswordCredential("someone", "mypassword".toCharArray()));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .setPasswordCredential(XenonProto.PasswordCredential.newBuilder()
                    .setUsername("someone")
                    .setPassword("mypassword"))
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_certAsDefault() throws XenonException {
        Credential cred = new CredentialMap(new CertificateCredential("someone","/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray()));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .setCertificateCredential(XenonProto.CertificateCredential.newBuilder()
                    .setUsername("someone")
                    .setCertfile("/home/someone/.ssh/id_rsa")
                    .setPassphrase("mypassphrase")
                )
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_keytabAsDefault() throws XenonException {
        Credential cred = new CredentialMap(new KeytabCredential("someone", "/home/someone/mycluster.keytab"));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .setKeytabCredential(XenonProto.KeytabCredential.newBuilder()
                                .setUsername("someone")
                                .setKeytabfile("/home/someone/mycluster.keytab"))
                )
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_unknownAsDefault() throws XenonException {
        thrown.expect(XenonException.class);
        thrown.expectMessage("Unknown credential class");

        Credential cred = new CredentialMap(getUnknownCredential());

        toCredentialResponse(cred);
    }

    @Test
    public void toCredentialResponse_map_defaultAsEntry() throws XenonException {
        CredentialMap cred = new CredentialMap();
        cred.put("somehost", new DefaultCredential("someoneelse"));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
            .setDefaultCredential(
                XenonProto.DefaultCredential.newBuilder().setUsername("someoneelse").build()
            ).build();
        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .putEntries("somehost", credEntry)
                .build()
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_passwordAsEntry() throws XenonException {
        CredentialMap cred = new CredentialMap();
        cred.put("somehost", new PasswordCredential("someone", "mypassword".toCharArray()));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
            .setPasswordCredential(XenonProto.PasswordCredential.newBuilder()
                .setUsername("someone")
                .setPassword("mypassword")
            ).build();
        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .putEntries("somehost", credEntry)
                .build()
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_certAsEntry() throws XenonException {
        CredentialMap cred = new CredentialMap();
        cred.put("somehost", new CertificateCredential("someone", "/home/someone/.ssh/id_rsa", "mypassphrase".toCharArray()));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
            .setCertificateCredential(XenonProto.CertificateCredential.newBuilder()
                .setUsername("someone")
                .setCertfile("/home/someone/.ssh/id_rsa")
                .setPassphrase("mypassphrase")
            ).build();
        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
            .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                .putEntries("somehost", credEntry)
                .build()
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_keytabAsEntry() throws XenonException {
        CredentialMap cred = new CredentialMap();
        cred.put("somehost", new KeytabCredential("someone", "/home/someone/mycluster.keytab"));

        XenonProto.GetCredentialResponse response = toCredentialResponse(cred);

        XenonProto.UserCredential credEntry = XenonProto.UserCredential.newBuilder()
                .setKeytabCredential(XenonProto.KeytabCredential.newBuilder()
                        .setUsername("someone")
                        .setKeytabfile("/home/someone/mycluster.keytab")
                ).build();
        XenonProto.GetCredentialResponse expected = XenonProto.GetCredentialResponse.newBuilder()
                .setCredentialMap(XenonProto.CredentialMap.newBuilder()
                        .putEntries("somehost", credEntry)
                        .build()
                )
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void toCredentialResponse_map_unknownAsEntry() throws XenonException {
        thrown.expect(XenonException.class);
        thrown.expectMessage("Unknown credential class");

        CredentialMap cred = new CredentialMap();
        cred.put("somehost", getUnknownCredential());

        toCredentialResponse(cred);
    }

    private UserCredential getUnknownCredential() {
        return new UserCredential() {
            @Override
            public String getUsername() {
                return null;
            }

            @Override
            public int hashCode() {
                return super.hashCode();
            }
        };
    }
}