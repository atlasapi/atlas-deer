package org.atlasapi.organisation;

import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.metabroadcast.common.ids.IdGenerator;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

public class IdSettingOrganisationStore implements OrganisationStore {

    private final OrganisationStore delegate;
    private final IdGenerator idGenerator;
    private static final int TIMEOUT_IN_SECONDS = 30;

    public IdSettingOrganisationStore(OrganisationStore delegate, IdGenerator idGenerator) {
        this.delegate = delegate;
        this.idGenerator = idGenerator;
    }

    @Override
    public ListenableFuture<Resolved<Organisation>> resolveIds(Iterable<Id> ids) {
        return delegate.resolveIds(ids);
    }

    @Override
    public Organisation write(Organisation organisation) {
        ensureId(organisation);
        return delegate.write(organisation);
    }

    private Organisation ensureId(Organisation organisation) {
        Optional<Id> possibleId;

        try {
            possibleId = getIdByUri(organisation).get(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        if (possibleId.isPresent()) {
            organisation.setId(possibleId.get());
        } else {
            organisation.setId(idGenerator.generateRaw());
        }
        return organisation;

    }

    @Override
    public ListenableFuture<Optional<Id>> getIdByUri(Organisation organisation) {
        return delegate.getIdByUri(organisation);
    }
}
