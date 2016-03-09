package org.atlasapi.content.v2.model.udt;

import java.util.Set;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "seriesref")
public class SeriesRef {

    @Field(name = "r")
    private Ref ref;

    @Field(name = "t")
    private String title;

    @Field(name = "u")
    private Instant updated;

    @Field(name = "sn")
    private Integer seriesNumber;

    @Field(name = "ry")
    private Integer releaseYear;

    @Field(name = "cr")
    private Set<Certificate> certificates;

    public Ref getRef() {
        return ref;
    }

    public void setRef(Ref ref) {
        this.ref = ref;
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
