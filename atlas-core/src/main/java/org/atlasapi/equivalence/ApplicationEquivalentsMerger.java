package org.atlasapi.equivalence;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface ApplicationEquivalentsMerger<E extends Equivalable<E>> {

    <T extends E> List<T> merge(Optional<Id> id, Iterable<T> equivalents, ApplicationSources sources);
    
}
