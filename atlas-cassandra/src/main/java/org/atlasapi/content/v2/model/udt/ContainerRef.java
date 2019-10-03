package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.comparison.ExcludeFromObjectComparison;
import org.joda.time.Instant;

import java.util.Set;

@UDT(name = "containerref")
public class ContainerRef {

    @Field(name = "id") private Long id;
    @Field(name = "src") private String source;
    @Field(name = "type") private String type;

    @Field(name = "title") private String title;
    @Field(name = "series_nr") private Integer seriesNumber;
    @ExcludeFromObjectComparison
    @Field(name = "updated") private Instant updated;
    @Field(name = "release_year") private Integer releaseYear;
    @Field(name = "certificates") private Set<Certificate> certificates;

    public ContainerRef() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

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

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    public void setSeriesNumber(Integer seriesNumber) {
        this.seriesNumber = seriesNumber;
    }

    public Integer getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(Integer releaseYear) {
        this.releaseYear = releaseYear;
    }

    public Set<Certificate> getCertificates() {
        return certificates;
    }

    public void setCertificates(
            Set<Certificate> certificates) {
        this.certificates = certificates;
    }
}
