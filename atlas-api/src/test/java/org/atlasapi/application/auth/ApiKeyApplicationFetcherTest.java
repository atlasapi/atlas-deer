package org.atlasapi.application.auth;

import com.google.api.client.util.Lists;
import com.metabroadcast.applications.client.ApplicationsClient;
import com.metabroadcast.applications.client.exceptions.ErrorCode;
import com.metabroadcast.applications.client.model.internal.AccessRoles;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.applications.client.model.internal.Environment;
import com.metabroadcast.applications.client.query.Query;
import com.metabroadcast.applications.client.query.Result;

import com.metabroadcast.common.servlet.StubHttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ApiKeyApplicationFetcherTest {

    @Mock private ApplicationsClient applicationsClient;

    private ApiKeyApplicationFetcher fetcher;
    private String apiKey;
    private Query query;

    @Before
    public void setup() {
        fetcher = new ApiKeyApplicationFetcher(applicationsClient);
        apiKey = "apiKey";
        query = Query.create(apiKey, Environment.PROD);
    }

    @Test
    public void testGetsSourcesForApiKeyAsParameter() throws Exception {

        Application application = getApplication();
        Result result = Result.success(application);
        HttpServletRequest request = new StubHttpServletRequest().withParam("key", apiKey);

        when(applicationsClient.resolve(query)).thenReturn(result);

        Optional<Application> app = fetcher.applicationFor(request);

        assertTrue(app.isPresent());
        assertThat(app.get(), is(application));
    }

    @Test
    public void testGetsSourcesForApiKeyInHeader() throws Exception {

        Application application = getApplication();
        Result result = Result.success(application);
        HttpServletRequest request = new StubHttpServletRequest().withHeader("key", apiKey);

        when(applicationsClient.resolve(query)).thenReturn(result);

        Optional<Application> app = fetcher.applicationFor(request);

        assertTrue(app.isPresent());
        assertThat(app.get(), is(application));
    }

    @Test
    public void testReturnsEmptyIfNoApiKeyIsSupplied() throws Exception {

        HttpServletRequest request = new StubHttpServletRequest();
        Optional<Application> app = fetcher.applicationFor(request);

        assertFalse(app.isPresent());
    }

    @Test(expected = InvalidApiKeyException.class)
    public void testThrowsInvalidApiKeyExceptionIfAppIsRevoked() throws Exception {

        Result result = Result.failure(ErrorCode.REVOKED);
        HttpServletRequest request = new StubHttpServletRequest().withParam("key", apiKey);

        when(applicationsClient.resolve(query)).thenReturn(result);

        fetcher.applicationFor(request);
    }

    @Test(expected = InvalidApiKeyException.class)
    public void testThrowsInvalidApiKeyExceptionIfTheresNoAppForKey() throws Exception {

        Result result = Result.failure(ErrorCode.NOT_FOUND);
        HttpServletRequest request = new StubHttpServletRequest().withParam("key", apiKey);

        when(applicationsClient.resolve(query)).thenReturn(result);

        fetcher.applicationFor(request);
    }

    private Application getApplication() {
        return Application.builder()
                .withId(-1l)
                .withTitle("test")
                .withDescription("desc")
                .withEnvironment(Environment.PROD)
                .withCreated(ZonedDateTime.now())
                .withApiKey(apiKey)
                .withSources(mock(ApplicationConfiguration.class))
                .withAllowedDomains(Lists.newArrayList())
                .withAccessRoles(mock(AccessRoles.class))
                .withRevoked(false)
                .build();
    }

}
