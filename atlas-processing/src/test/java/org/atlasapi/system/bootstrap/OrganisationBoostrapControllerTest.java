package org.atlasapi.system.bootstrap;

import java.math.BigInteger;

import javax.servlet.http.HttpServletResponse;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.organisation.OrganisationWriter;

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
public class OrganisationBoostrapControllerTest {

    private @Mock OrganisationResolver resolver;
    private @Mock OrganisationWriter writer;
    private @Mock NumberToShortStringCodec idCodec;
    private @Mock HttpServletResponse response;
    private @Mock Organisation event;

    private OrganisationBoostrapController controller;

    private String id;
    private long encodedId;

    @Before
    public void setUp() throws Exception {
        controller = new OrganisationBoostrapController(resolver, writer, idCodec);

        id = "0";
        encodedId = 0L;

        when(idCodec.decode(id)).thenReturn(BigInteger.valueOf(encodedId));

    }

    @Test
    public void testBootstrapEvent() throws Exception {
        when(resolver.resolveIds(ImmutableList.of(Id.valueOf(encodedId))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(event))));

        controller.bootstrapOrganisation(id, response);

        verify(writer).write(event);
        verify(response).setStatus(HttpStatus.OK.value());
    }

    @Test
    public void testBootstrapMissingEvent() throws Exception {
        when(resolver.resolveIds(any())).thenReturn(Futures.immediateFuture(Resolved.valueOf(
                ImmutableList.of())));

        controller.bootstrapOrganisation(id, response);

        verify(writer, never()).write(any());
        verify(response).sendError(HttpStatus.NOT_FOUND.value());
    }

}