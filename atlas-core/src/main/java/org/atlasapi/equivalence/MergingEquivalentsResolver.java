package org.atlasapi.equivalence;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;

import com.google.common.util.concurrent.ListenableFuture;

public interface MergingEquivalentsResolver<E extends Equivalable<E>> {

    ListenableFuture<ResolvedEquivalents<E>> resolveIds(
            Iterable<Id> ids,
            Application application,
            Set<Annotation> activeAnnotations,
            Set<AttributeQuery<?>> operands
    );

}
