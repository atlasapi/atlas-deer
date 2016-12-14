package org.atlasapi.query.annotation;

import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.common.Resource;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ForwardingSetMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

public final class ActiveAnnotations extends ForwardingSetMultimap<List<Resource>, Annotation> {

    private final ImmutableSetMultimap<List<Resource>, Annotation> activeAnnotations;

    private static final ActiveAnnotations STANDARD = new ActiveAnnotations(
            toMultimap(
                    Maps.toMap(
                            Lists.transform(
                                    Resource.all().asList(),
                                    ImmutableList::of
                            ),
                            Functions.constant(Annotation.standard())
                    )
            )
    );

    public ActiveAnnotations(SetMultimap<List<Resource>, Annotation> activeAnnotations) {
        this.activeAnnotations = ImmutableSetMultimap.copyOf(activeAnnotations);
    }

    public static final ActiveAnnotations standard() {
        return STANDARD;
    }

    private static SetMultimap<List<Resource>, Annotation> toMultimap(
            ImmutableMap<List<Resource>, ImmutableSet<Annotation>> standard) {
        ImmutableSetMultimap.Builder<List<Resource>, Annotation> std
                = ImmutableSetMultimap.builder();
        for (Entry<List<Resource>, ImmutableSet<Annotation>> entry : standard.entrySet()) {
            std.putAll(entry.getKey(), entry.getValue());
        }
        return std.build();
    }


    @Override
    protected SetMultimap<List<Resource>, Annotation> delegate() {
        return activeAnnotations;
    }

    public ImmutableSet<Annotation> forPath(List<Resource> resources) {
        return activeAnnotations.get(resources);
    }

    public ImmutableSet<Annotation> all() {
        return ImmutableSet.copyOf(activeAnnotations.values());
    }

}
