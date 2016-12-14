package org.atlasapi.content;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface ContainerSummaryResolver {

    Optional<ContainerSummary> resolveContainerSummary(Id id, Application application,
            Set<Annotation> annotations);
}
