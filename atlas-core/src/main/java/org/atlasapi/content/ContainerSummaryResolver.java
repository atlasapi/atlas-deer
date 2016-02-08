package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;

import com.google.common.base.Optional;

public interface ContainerSummaryResolver {

    Optional<ContainerSummary> resolveContainerSummary(Id id, ApplicationSources applicationSources,
            Set<Annotation> annotations);
}
