package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.api.client.repackaged.com.google.common.base.Objects;

public final class ItemDisplayTitleWriter implements EntityWriter<Item> {

    @Override
    public void write(Item item, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("title", title(item));
        writer.writeField("subtitle", subtitle(item));
    }

    private String title(Item item) {
        if (isSpecial(item) || !hasContainerTitle(item)) {
            return item.getTitle();
        }
        return item.getContainerSummary().getTitle();
    }

    private String subtitle(Item item) {
        if (isSpecial(item) || !hasContainerTitle(item)) {
            return null;
        }
        if (hasSeriesSummary(item)) {
            String episodeTitle = episodeTitle(item, item.getContainerSummary());
            return seriesEpisodeTitle(episodeTitle, item.getContainerSummary(), seriesRef(item));
        }
        return episodeTitle(item, item.getContainerSummary());
    }

    private String seriesEpisodeTitle(String episodeTitle, ContainerSummary containerSummary,
            SeriesRef seriesRef) {
        if (episodeTitle == null) {
            return null;
        }
        if (seriesRef.getTitle() != null && !seriesRef.getTitle()
                .equals(containerSummary.getTitle())) {
            return String.format("%s, %s", seriesRef.getTitle(), episodeTitle);
        }
        if (seriesRef.getSeriesNumber() != null) {
            return String.format("Series %s, %s", seriesRef.getSeriesNumber(), episodeTitle);
        }
        return episodeTitle;
    }

    private SeriesRef seriesRef(Item item) {
        return ((Episode) item).getSeriesRef();
    }

    private boolean hasSeriesSummary(Item item) {
        return item instanceof Episode && ((Episode) item).getSeriesRef() != null;
    }

    private String episodeTitle(Item item, ContainerSummary containerSummary) {
        String itemTitle = item.getTitle();
        String containerTitle = containerSummary.getTitle();
        Integer episodeNumber = episodeNumber(item);
        if (itemTitle != null && !itemTitle.equals(containerTitle)) {
            if (episodeNumber != null && !sequenceTitle(itemTitle) && !dateTitle(itemTitle)) {
                return String.format("Episode %s: %s", episodeNumber, itemTitle);
            }
            return itemTitle;
        }
        if (episodeNumber != null) {
            return String.format("Episode %s", episodeNumber);
        }
        if (item.getBroadcasts() !=null && !item.getBroadcasts().isEmpty()) {
            return firstBroadcast(item).getTransmissionTime().toString("dd/MM/yyyy");
        }
        return null;
    }

    private Broadcast firstBroadcast(Item item) {
        return Broadcast.startTimeOrdering().min(item.getBroadcasts());
    }

    private boolean dateTitle(String itemTitle) {
        return itemTitle.matches("\\d{2}/\\d{2}/\\d{4}");
    }

    private boolean sequenceTitle(String itemTitle) {
        return itemTitle.matches("Episode \\d+");
    }

    private Integer episodeNumber(Item item) {
        if (item instanceof Episode) {
            return ((Episode) item).getEpisodeNumber();
        }
        return null;
    }

    private boolean hasContainerTitle(Item item) {
        return hasContainerSummary(item) && item.getContainerSummary().getTitle() != null;
    }

    private boolean hasContainerSummary(Item item) {
        return item.getContainerSummary() != null;
    }

    private boolean isSpecial(Item item) {
        return item instanceof Episode && Objects.firstNonNull(
                ((Episode) item).getSpecial(),
                Boolean.FALSE
        );
    }

    @Override
    public String fieldName(Item item) {
        return "display_title";
    }
}