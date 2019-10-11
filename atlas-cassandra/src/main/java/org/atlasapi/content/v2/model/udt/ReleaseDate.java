package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.LocalDate;

import java.util.Objects;

@UDT(name = "releasedate")
public class ReleaseDate {

    @Field(name = "country") private String country;
    @Field(name = "type") private String type;
    @Field(name = "release_date") private LocalDate releaseDate;

    public ReleaseDate() {}

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public LocalDate getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(LocalDate releaseDate) {
        this.releaseDate = releaseDate;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ReleaseDate that = (ReleaseDate) object;
        return Objects.equals(country, that.country) &&
                Objects.equals(type, that.type) &&
                Objects.equals(releaseDate, that.releaseDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, type, releaseDate);
    }
}
