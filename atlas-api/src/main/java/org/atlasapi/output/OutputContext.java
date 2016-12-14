package org.atlasapi.output;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Region;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.useraware.UserAccountsAwareQueryContext;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contains state required during the output of a response. Not thread-safe
 */
public class OutputContext {

    public static OutputContext valueOf(QueryContext standard) {
        return new OutputContext(
                standard.getAnnotations(),
                standard.getApplication(),
                standard.getRequest(),
                null
        );
    }

    public static OutputContext valueOf(QueryContext standard, Region region) {
        return new OutputContext(
                standard.getAnnotations(),
                standard.getApplication(),
                standard.getRequest(),
                region
        );
    }

    public static OutputContext valueOf(UserAwareQueryContext standard) {
        return new OutputContext(
                standard.getAnnotations(),
                standard.getApplication(),
                standard.getRequest(),
                null
        );
    }

    public static OutputContext valueOf(UserAccountsAwareQueryContext standard) {
        return new OutputContext(
                standard.getAnnotations(),
                standard.getApplication(),
                standard.getRequest(),
                null
        );
    }

    private final ActiveAnnotations annotations;
    private final Application application;
    private final List<Resource> resources;
    private final HttpServletRequest request;
    private final Optional<Region> region;

    public OutputContext(
            ActiveAnnotations activeAnnotations,
            Application application,
            HttpServletRequest request,
            @Nullable Region region
    ) {
        this.annotations = checkNotNull(activeAnnotations);
        this.application = checkNotNull(application);
        this.resources = Lists.newLinkedList();
        this.request = checkNotNull(request);
        this.region = Optional.ofNullable(region);
    }

    public final OutputContext startResource(Resource resource) {
        resources.add(resource);
        return this;
    }

    public final OutputContext endResource() {
        resources.remove(resources.size() - 1);
        return this;
    }

    public ImmutableSet<Annotation> getActiveAnnotations() {
        return annotations.forPath(resources);
    }

    public <T> List<OutputAnnotation<? super T>> getAnnotations(AnnotationRegistry<T> registry) {
        ImmutableSet<Annotation> active = annotations.forPath(resources);
        if (active == null || active.isEmpty()) {
            return registry.defaultAnnotations();
        }

        return registry.activeAnnotations(active);
    }

    public Application getApplication() {
        return this.application;
    }

    public HttpServletRequest getRequest() {
        return request;
    }

    public Optional<Region> getRegion() {
        return region;
    }
}
