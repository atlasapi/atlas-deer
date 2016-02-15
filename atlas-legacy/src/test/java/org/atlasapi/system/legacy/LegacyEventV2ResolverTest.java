package org.atlasapi.system.legacy;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.eventV2.EventV2Resolver;
import org.atlasapi.media.entity.Event;
import org.atlasapi.persistence.event.EventResolver;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LegacyEventV2ResolverTest {

    private @Mock EventResolver eventResolver;
    private @Mock LegacyEventV2Transformer legacyEventTransformer;
    private @Mock Event legacyEvent;
    private @Mock org.atlasapi.eventV2.EventV2 event;

    private LegacyEventV2Resolver legacyEventResolver;

    @Before
    public void setUp() throws Exception {
        legacyEventResolver = new LegacyEventV2Resolver(eventResolver, legacyEventTransformer);
    }

    @Test
    public void testResolveIds() throws Exception {
        when(eventResolver.fetch(0L)).thenReturn(Optional.of(legacyEvent));
        when(eventResolver.fetch(1L)).thenReturn(Optional.<Event>absent());
        when(legacyEventTransformer.apply(legacyEvent)).thenReturn(event);

        ListenableFuture<Resolved<org.atlasapi.eventV2.EventV2>> future = legacyEventResolver
                .resolveIds(ImmutableList.of(Id.valueOf(0L), Id.valueOf(1L)));

        FluentIterable<org.atlasapi.eventV2.EventV2> resources = future.get().getResources();
        assertThat(resources.size(), is(1));
        assertThat(resources.first().get(), sameInstance(event));
    }

}