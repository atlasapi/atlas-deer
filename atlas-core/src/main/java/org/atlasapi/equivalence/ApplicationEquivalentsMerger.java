package org.atlasapi.equivalence;

import java.util.List;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface ApplicationEquivalentsMerger<E extends Equivalable<E>> {

    <T extends E> List<T> merge(Optional<Id> id, Iterable<T> equivalents,
            Application application);

}
