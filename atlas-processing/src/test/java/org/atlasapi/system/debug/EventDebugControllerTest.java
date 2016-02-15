package org.atlasapi.system.debug;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.AtlasPersistenceModule;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.system.legacy.LegacyEventV2Resolver;
import org.atlasapi.system.legacy.LegacyPersistenceModule;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventDebugControllerTest {

    @Mock
    private LegacyPersistenceModule legacyPersistenceModule;
    @Mock
    private AtlasPersistenceModule atlasPersistenceModule;
    @Mock
    private LegacyEventV2Resolver resolver;
    private NumberToShortStringCodec lowercaseDecoder;
    private Gson gson = new GsonBuilder().registerTypeAdapter(
            DateTime.class,
            (JsonSerializer<DateTime>) (src, typeOfSrc, context) ->
                    new JsonPrimitive(src.toString())
    )
            .create();;

    private EventDebugController controller;



    @Before
    public void setUp() {
        lowercaseDecoder = SubstitutionTableNumberCodec.lowerCaseOnly();
        when(atlasPersistenceModule.eventV2Resolver()).thenReturn(resolver);
        controller = new EventDebugController(legacyPersistenceModule, atlasPersistenceModule);
    }

    @Test
    public void testDeserializerDoesntThrowException() throws Exception {
        EventV2 eventV2 = EventV2.builder().withId(Id.valueOf(1l)).withTitle("title").withCanonicalUri("uri").withSource(
                Publisher.BBC).build();
        when(resolver.resolveIds(anyCollection())).thenReturn(Futures.immediateFuture(Resolved.valueOf(
                ImmutableList.of(eventV2))));
        HttpServletResponse response = mock(HttpServletResponse.class);
        PrintWriter printWriter = new PrintWriter(new StringWriter());
        when(response.getWriter()).thenReturn(printWriter);
        controller.printEventV2("hnv5", response);
    }

}