package org.atlasapi.system.legacy;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.persistence.content.organisation.OrganisationStore;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

@RunWith(MockitoJUnitRunner.class)
public class LegacyOrganisationResolverTest {
    private @Mock OrganisationStore store;
    private @Mock LegacyOrganisationTransformer transformer;
    private Organisation legacyOrganisation;
    private org.atlasapi.organisation.Organisation organisation;

    private LegacyOrganisationResolver resolver;

    @Before
    public void setUp() {
        transformer =  new LegacyOrganisationTransformer();
        resolver = new LegacyOrganisationResolver(store, transformer);
    }

    @Test
    public void testResolvingOrganisations() throws Throwable {
        organisation = new org.atlasapi.organisation.Organisation();
        legacyOrganisation = new Organisation();
        legacyOrganisation.setAlternativeTitles(ImmutableList.of("title1", "title2"));
        when(store.organisation(1l)).thenReturn(Optional.of(legacyOrganisation));
        ListenableFuture<Resolved<org.atlasapi.organisation.Organisation>> resources = resolver.resolveIds(ImmutableList.of(Id.valueOf(1L)));
        FluentIterable<org.atlasapi.organisation.Organisation> organisations = resources.get().getResources();
        assertThat(organisations.size(), is(1));
        assertThat(organisations.first().get().getAlternativeTitles(), is(legacyOrganisation.getAlternativeTitles()));
    }
}