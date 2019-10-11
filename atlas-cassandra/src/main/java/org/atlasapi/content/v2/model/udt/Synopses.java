package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "synopses")
public class Synopses {

    @Field(name = "short") private String shortDescr;
    @Field(name = "medium") private String mediumDescr;
    @Field(name = "long") private String longDescr;

    public Synopses() {}

    public String getShortDescr() {
        return shortDescr;
    }

    public void setShortDescr(String shortDescr) {
        this.shortDescr = shortDescr;
    }

    public String getMediumDescr() {
        return mediumDescr;
    }

    public void setMediumDescr(String mediumDescr) {
        this.mediumDescr = mediumDescr;
    }

    public String getLongDescr() {
        return longDescr;
    }

    public void setLongDescr(String longDescr) {
        this.longDescr = longDescr;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Synopses synopses = (Synopses) object;
        return Objects.equals(shortDescr, synopses.shortDescr) &&
                Objects.equals(mediumDescr, synopses.mediumDescr) &&
                Objects.equals(longDescr, synopses.longDescr);
    }
}
