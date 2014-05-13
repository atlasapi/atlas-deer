package org.atlasapi.application.auth;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.ApplicationStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

@RunWith(MockitoJUnitRunner.class)
public class ApiKeySourcesFetcherTest {

    @Mock ApplicationStore appStore;
    ApiKeySourcesFetcher fetcher;
    
    @Before
    public void setup() {
        fetcher = new ApiKeySourcesFetcher(appStore);
    }
    
    @Test
    public void testGetsSourcesForApiKey() throws Exception {
        
        String apiKey = "apikey";
        Application app = Application.builder()
            .withCredentials(ApplicationCredentials.builder().withApiKey(apiKey).build())
            .build();
        when(appStore.applicationForKey(apiKey)).thenReturn(Optional.of(app));
        
        Optional<ApplicationSources> srcs = fetcher.sourcesFor(new StubHttpServletRequest().withParam("key", apiKey));
        
        assertTrue(srcs.isPresent());
        assertThat(srcs.get(), is(app.getSources()));
    }

    @Test
    public void testReturnsAbsentIfNoApiKeyIsSupplied() throws Exception {
        
        String apiKey = "apikey";
        Application app = Application.builder()
                .withCredentials(ApplicationCredentials.builder().withApiKey(apiKey).build())
                .build();
        when(appStore.applicationForKey(apiKey)).thenReturn(Optional.of(app));
        
        Optional<ApplicationSources> srcs = fetcher.sourcesFor(new StubHttpServletRequest());
        
        verify(appStore, never()).applicationForKey(apiKey);
        assertFalse(srcs.isPresent());
    }

    @Test(expected=InvalidApiKeyException.class)
    public void testThrowsInvalidApiKeyExceptionIfAppIsRevoked() throws Exception {
        
        String apiKey = "apikey";
        Application app = Application.builder()
            .withCredentials(ApplicationCredentials.builder().withApiKey(apiKey).build())
            .withRevoked(true)
            .build();
        when(appStore.applicationForKey(apiKey)).thenReturn(Optional.of(app));
        
        fetcher.sourcesFor(new StubHttpServletRequest().withParam("key", apiKey));
    }
    
    @Test(expected=InvalidApiKeyException.class)
    public void testThrowsInvalidApiKeyExceptionIfTheresNoAppForKey() throws Exception {
        
        String apiKey = "apikey";
        when(appStore.applicationForKey(apiKey)).thenReturn(Optional.<Application>absent());
        
        fetcher.sourcesFor(new StubHttpServletRequest().withParam("key", apiKey));
        
    }

}
