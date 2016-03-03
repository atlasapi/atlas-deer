package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.LocalDate;

@UDT(name = "releasedate")
public class ReleaseDate {

    private String country;
    private String type;
    private LocalDate releaseDate;

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
}
