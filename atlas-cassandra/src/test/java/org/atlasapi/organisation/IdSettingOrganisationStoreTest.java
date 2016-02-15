package org.atlasapi.organisation;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.IdGenerator;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IdSettingOrganisationStoreTest {

    @Mock
    private OrganisationStore organisationStore;
    @Mock
    private IdGenerator idGenerator;
    @Captor
    private ArgumentCaptor<Organisation> organisationCaptor;
    private OrganisationStore idSettingStore;

    @Before
    public void setUp() {
        when(idGenerator.generateRaw()).thenReturn(1l);
        idSettingStore = new IdSettingOrganisationStore(organisationStore, idGenerator);
    }

    @Test
    public void testIdStoreSetsId() {
        Organisation organisation = new Organisation();
        organisation.setId(Id.valueOf(2l));
        organisation.setCanonicalUri("uri");
        when(organisationStore.getExistingId(organisation)).thenReturn(Futures.immediateFuture(Optional.absent()));
        idSettingStore.write(organisation);
        verify(organisationStore).write(organisationCaptor.capture());
        assertThat(organisationCaptor.getValue().getId(), is(Id.valueOf(1l)));
    }

    @Test
    public void testIdStoreNotSetsId() {
        Organisation organisation = new Organisation();
        organisation.setId(Id.valueOf(2l));
        organisation.setCanonicalUri("uri");
        when(organisationStore.getExistingId(organisation)).thenReturn(Futures.immediateFuture(Optional.of(Id.valueOf(2l))));
        idSettingStore.write(organisation);
        verify(organisationStore).write(organisationCaptor.capture());
        assertThat(organisationCaptor.getValue().getId(), is(Id.valueOf(2l)));
    }

}