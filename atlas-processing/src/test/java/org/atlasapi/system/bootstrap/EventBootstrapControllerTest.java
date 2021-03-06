package org.atlasapi.system.bootstrap;

import java.math.BigInteger;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.event.Event;
import org.atlasapi.event.EventResolver;
import org.atlasapi.event.EventWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EventBootstrapControllerTest {

    private @Mock EventResolver eventResolver;
    private @Mock EventWriter eventWriter;
    private @Mock NumberToShortStringCodec idCodec;
    private @Mock HttpServletResponse response;
    private @Mock Event event;

    private EventBootstrapController controller;

    private String id;
    private long encodedId;

    @Before
    public void setUp() throws Exception {
        controller = new EventBootstrapController(eventResolver, idCodec, eventWriter);

        id = "0";
        encodedId = 0L;

        when(idCodec.decode(id)).thenReturn(BigInteger.valueOf(encodedId));

    }

    @Test
    public void testBootstrapEvent() throws Exception {
        when(eventResolver.resolveIds(ImmutableList.of(Id.valueOf(encodedId))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(event))));

        controller.bootstrapEvent(id, response);

        verify(eventWriter).write(event);
        verify(response).setStatus(HttpStatus.OK.value());
    }

    @Test
    public void testBootstrapMissingEvent() throws Exception {
        when(eventResolver.resolveIds(any())).thenReturn(Futures.immediateFuture(Resolved.valueOf(
                ImmutableList.of())));

        controller.bootstrapEvent(id, response);

        verify(eventWriter, never()).write(any());
        verify(response).sendError(HttpStatus.NOT_FOUND.value());
    }
}