package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "containersummary")
public class ContainerSummary {

    @Field(name = "type") private String type;
    @Field(name = "title") private String title;
    @Field(name = "descr") private String description;
    @Field(name = "series_nr") private Integer seriesNumber;
    @Field(name = "total_episodes") private Integer totalEpisodes;

    public ContainerSummary() {}

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

    public Integer getTotalEpisodes() {
        return totalEpisodes;
    }

    public void setTotalEpisodes(Integer totalEpisodes) {
        this.totalEpisodes = totalEpisodes;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ContainerSummary that = (ContainerSummary) object;
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
