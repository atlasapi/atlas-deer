package org.atlasapi.output.writers.common;

import java.io.IOException;

import org.atlasapi.content.ContainerSummary;
import org.atlasapi.output.FieldWriter;

public class CommonContainerSummaryWriter {

    private CommonContainerSummaryWriter() { }

    public static CommonContainerSummaryWriter create() {
        return new CommonContainerSummaryWriter();
    }

    public void write(ContainerSummary containerSummary, FieldWriter writer) throws IOException {
        writer.writeField("type", containerSummary.getType().toLowerCase());
        writer.writeField("title", containerSummary.getTitle());
        writer.writeField("description", containerSummary.getDescription());
        if (containerSummary.getSeriesNumber() != null) {
            writer.writeField("series_number", containerSummary.getSeriesNumber());
        }
        if (containerSummary.getTotalEpisodes() != null) {
            writer.writeField("total_episodes", containerSummary.getTotalEpisodes());
        }
    }
}
