package org.atlasapi.content;

import org.atlasapi.meta.annotations.FieldName;

public class ContainerSummary {

    //TODO add ID here
    private final String type;
    private final String title;
    private final String description;
    private final Integer seriesNumber;

    public ContainerSummary(String type, String title, String description, Integer seriesNumber) {
        this.type = type;
        this.title = title;
        this.description = description;
        this.seriesNumber = seriesNumber;
    }

    @FieldName("type")
    public String getType() {
        return type;
    }

    @FieldName("title")
    public String getTitle() {
        return title;
    }

    @FieldName("description")
    public String getDescription() {
        return description;
    }

    @FieldName("series_number")
    public Integer getSeriesNumber() {
        return seriesNumber;
    }
}