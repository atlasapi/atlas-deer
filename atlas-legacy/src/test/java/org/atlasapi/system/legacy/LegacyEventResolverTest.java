package org.atlasapi.system.legacy;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
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
public class LegacyEventResolverTest {

    private @Mock EventResolver eventResolver;
    private @Mock LegacyEventTransformer legacyEventTransformer;
    private @Mock Event legacyEvent;
    private @Mock org.atlasapi.event.Event event;

    private LegacyEventResolver legacyEventResolver;

    @Before
    public void setUp() throws Exception {
        legacyEventResolver = new LegacyEventResolver(eventResolver, legacyEventTransformer);
    }

    @Test
    public void testResolveIds() throws Exception {
        when(eventResolver.fetch(0L)).thenReturn(Optional.of(legacyEvent));
        when(eventResolver.fetch(1L)).thenReturn(Optional.<Event>absent());
        when(legacyEventTransformer.apply(legacyEvent)).thenReturn(event);

        ListenableFuture<Resolved<org.atlasapi.event.Event>> future = legacyEventResolver
                .resolveIds(ImmutableList.of(Id.valueOf(0L), Id.valueOf(1L)));

        FluentIterable<org.atlasapi.event.Event> resources = future.get().getResources();
        assertThat(resources.size(), is(1));
        assertThat(resources.first().get(), sameInstance(event));
    }

}