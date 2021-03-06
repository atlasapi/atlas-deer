package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.util.NullOrEmptyEquality;
import org.joda.time.Instant;

import java.util.Objects;
import java.util.Set;

/** This doesn't hold the actual ID and publisher because those are the strict PK of
 * any resource ref. These objects are usually stored as a CQL {@code map<Ref, PartialItemRef>} and
 * serialised accordingly. If you need to store a full {@code PartialItemRef} as a field, you'll need a
 * to make a different UDT.
 *
 * @see Ref
 * @see PartialItemRef
 */
@UDT(name = "seriesref")
public class SeriesRef {

    @Field(name = "title") private String title;
    @Field(name = "updated") private Instant updated;
    @Field(name = "series_nr") private Integer seriesNumber;
    @Field(name = "release_year") private Integer releaseYear;
    @Field(name = "certificates") private Set<Certificate> certificates;

    public SeriesRef() {}

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

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        SeriesRef seriesRef = (SeriesRef) object;
        return Objects.equals(title, seriesRef.title) &&
                Objects.equals(seriesNumber, seriesRef.seriesNumber) &&
                Objects.equals(releaseYear, seriesRef.releaseYear) &&
                NullOrEmptyEquality.equals(certificates, seriesRef.certificates);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(title, seriesNumber, releaseYear, certificates);
    }
}
