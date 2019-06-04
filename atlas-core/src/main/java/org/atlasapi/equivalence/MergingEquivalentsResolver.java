package org.atlasapi.equivalence;

import java.util.Set;

import javax.annotation.Nullable;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;

import com.google.common.util.concurrent.ListenableFuture;

public interface MergingEquivalentsResolver<E extends Equivalable<E>> {

    ListenableFuture<ResolvedEquivalents<E>> resolveIds(
            Iterable<Id> ids,
            Application application,
            Set<Annotation> activeAnnotations,
            AttributeQuerySet operands
    );

}
