package org.atlasapi.content;

import java.util.Objects;

import javax.annotation.Nullable;

import org.atlasapi.meta.annotations.FieldName;

public class ContainerSummary {

    private final String type;
    private final String title;
    private final String description;
    private final Integer seriesNumber;
    private final Integer totalEpisodes;

    private ContainerSummary(
            @Nullable String type,
            @Nullable String title,
            @Nullable String description,
            @Nullable Integer seriesNumber,
            @Nullable Integer totalEpisodes
    ) {
        this.type = type;
        this.title = title;
        this.description = description;
        this.seriesNumber = seriesNumber;
        this.totalEpisodes = totalEpisodes;
    }

    public static ContainerSummary create(
            @Nullable String type,
            @Nullable String title,
            @Nullable String description,
            @Nullable Integer seriesNumber,
            @Nullable Integer totalEpisodes
    ) {
        return new ContainerSummary(type, title, description, seriesNumber, totalEpisodes);
    }

    public static ContainerSummary from(Series series) {
        return new ContainerSummary(
                Series.class.getSimpleName().toLowerCase(),
                series.getTitle(),
                series.getDescription(),
                series.getSeriesNumber(),
                series.getTotalEpisodes()
        );
    }

    public static ContainerSummary from(Brand brand) {
        return new ContainerSummary(
                Brand.class.getSimpleName().toLowerCase(),
                brand.getTitle(),
                brand.getDescription(),
                null,
                null
        );
    }

    @Nullable
    @FieldName("type")
    public String getType() {
        return type;
    }

    @Nullable
    @FieldName("title")
    public String getTitle() {
        return title;
    }

    @Nullable
    @FieldName("description")
    public String getDescription() {
        return description;
    }

    @Nullable
    @FieldName("series_number")
    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    @Nullable
    @FieldName("total_episodes")
    public Integer getTotalEpisodes() {
        return totalEpisodes;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerSummary that = (ContainerSummary) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(title, that.title) &&
                Objects.equals(description, that.description) &&
                Objects.equals(seriesNumber, that.seriesNumber) &&
                Objects.equals(totalEpisodes, that.totalEpisodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, title, description, seriesNumber, totalEpisodes);
    }
}
