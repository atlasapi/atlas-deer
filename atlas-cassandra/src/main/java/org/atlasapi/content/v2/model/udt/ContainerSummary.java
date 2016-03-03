package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "containersummary")
public class ContainerSummary {

    private String type;
    private String title;
    private String description;
    private Integer seriesNumber;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public void setSeriesNumber(Integer seriesNumber) {
        this.seriesNumber = seriesNumber;
    }
}
