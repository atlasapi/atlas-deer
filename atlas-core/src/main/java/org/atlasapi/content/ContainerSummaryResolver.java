package org.atlasapi.content;

import com.google.common.base.Optional;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Id;

public interface ContainerSummaryResolver {

    Optional<ContainerSummary> resolveContainerSummary(Id id, ApplicationSources applicationSources);
}
