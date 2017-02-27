package org.atlasapi.output;

import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.output.annotation.OutputAnnotation;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.Resource;
import org.atlasapi.query.common.context.QueryContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Contains state required during the output of a response. Not thread-safe
 */
public class OutputContext {

    public static OutputContext valueOf(QueryContext standard) {
        return builder(standard).build();
    }

    private final ActiveAnnotations annotations;
    private final Application application;
    private final List<Resource> resources;
    private final HttpServletRequest request;
    private final Optional<Region> region;
    private final Optional<Platform> platform;

    private OutputContext(Builder builder) {
        this.annotations = checkNotNull(builder.activeAnnotations);
        this.application = checkNotNull(builder.application);
        this.resources = Lists.newLinkedList();
        this.request = checkNotNull(builder.request);
        this.region = checkNotNull(builder.region);
        this.platform = checkNotNull(builder.platform);
    }

    public static Builder builder(QueryContext queryContext) {
        return new Builder(queryContext);
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

    public Optional<Platform> getPlatform() { return platform; }

    public static class Builder {

        private final QueryContext queryContext;
        private ActiveAnnotations activeAnnotations;
        private Application application;
        private HttpServletRequest request;
        private Optional<Region> region = Optional.empty();
        private Optional<Platform> platform = Optional.empty();

        private Builder(QueryContext queryContext) {
            this.queryContext = checkNotNull(queryContext);
        }

        public Builder withActiveAnnotations(ActiveAnnotations activeAnnotations) {
            this.activeAnnotations = activeAnnotations;
            return this;
        }

        public Builder withApplication(Application application) {
            this.application = application;
            return this;
        }

        public Builder withRequest(HttpServletRequest request) {
            this.request = request;
            return this;
        }

        public Builder withRegion(Region region) {
            this.region = Optional.of(region);
            return this;
        }

        public Builder withPlatform(Platform platform) {
            this.platform = Optional.of(platform);
            return this;
        }

        public OutputContext build() {
            if (activeAnnotations == null) {
                activeAnnotations = queryContext.getAnnotations();
            }

            if (application == null) {
                application = queryContext.getApplication();
            }

            if (request == null) {
                request = queryContext.getRequest();
            }

            return new OutputContext(this);
        }


    }
}
