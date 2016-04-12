package org.atlasapi.system.bootstrap;

import java.math.BigInteger;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.eventV2.EventV2Writer;

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

    private @Mock EventV2Resolver v2Resolver;
    private @Mock EventV2Writer v2Writer;
    private @Mock NumberToShortStringCodec idCodec;
    private @Mock HttpServletResponse response;
    private @Mock EventV2 event;

    private EventBootstrapController controller;

    private String id;
    private long encodedId;

    @Before
    public void setUp() throws Exception {
        controller = new EventBootstrapController(v2Resolver, idCodec, v2Writer);

        id = "0";
        encodedId = 0L;

        when(idCodec.decode(id)).thenReturn(BigInteger.valueOf(encodedId));

    }

    @Test
    public void testBootstrapEvent() throws Exception {
        when(v2Resolver.resolveIds(ImmutableList.of(Id.valueOf(encodedId))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(event))));

        controller.bootstrapEventV2(id, response);

        verify(v2Writer).write(event);
        verify(response).setStatus(HttpStatus.OK.value());
    }

    @Test
    public void testBootstrapMissingEvent() throws Exception {
        when(v2Resolver.resolveIds(any())).thenReturn(Futures.immediateFuture(Resolved.valueOf(
                ImmutableList.of())));

        controller.bootstrapEventV2(id, response);

        verify(v2Writer, never()).write(any());
        verify(response).sendError(HttpStatus.NOT_FOUND.value());
    }
}