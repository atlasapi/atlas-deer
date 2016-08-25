package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.ContainerSummary;

public class ContainerSummarySerialization {

    public ContainerSummary serialize(org.atlasapi.content.ContainerSummary itemContainerSummary) {
        if (itemContainerSummary == null) {
            return null;
        }

        ContainerSummary containerSummary = new ContainerSummary();
        containerSummary.setType(itemContainerSummary.getType());
        containerSummary.setTitle(itemContainerSummary.getTitle());
        containerSummary.setSeriesNumber(itemContainerSummary.getSeriesNumber());
        containerSummary.setDescription(itemContainerSummary.getDescription());
        containerSummary.setTotalEpisodes(itemContainerSummary.getTotalEpisodes());

        return containerSummary;
    }

    public org.atlasapi.content.ContainerSummary deserialize(ContainerSummary internal) {
        if (internal == null) {
            return null;
        }

        return org.atlasapi.content.ContainerSummary.create(
                internal.getType(),
                internal.getTitle(),
                internal.getDescription(),
                internal.getSeriesNumber(),
                internal.getTotalEpisodes()
        );
    }
}