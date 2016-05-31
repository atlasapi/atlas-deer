package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Summary;

public class ContainerSummarySerializer {

    public ContentProtos.Summary serialize(ContainerSummary containerSummary) {
        ContentProtos.Summary.Builder summary = ContentProtos.Summary.newBuilder();
        if (containerSummary.getType() != null) {
            summary.setType(containerSummary.getType());
        }
        if (containerSummary.getTitle() != null) {
            summary.setTitle(containerSummary.getTitle());
        }
        if (containerSummary.getDescription() != null) {
            summary.setDescription(containerSummary.getDescription());
        }
        if (containerSummary.getSeriesNumber() != null) {
            summary.setSeriesNumber(containerSummary.getSeriesNumber());
        }
        if (containerSummary.getTotalEpisodes() != null) {
            summary.setTotalEpisodes(containerSummary.getTotalEpisodes());
        }
        return summary.build();
    }

    public ContainerSummary deserialize(Summary summary) {
        return ContainerSummary.create(
                summary.hasType() ? summary.getType() : null,
                summary.hasTitle() ? summary.getTitle() : null,
                summary.hasDescription() ? summary.getDescription() : null,
                summary.hasSeriesNumber() ? summary.getSeriesNumber() : null,
                summary.hasTotalEpisodes() ? summary.getTotalEpisodes() : null
        );
    }
}
