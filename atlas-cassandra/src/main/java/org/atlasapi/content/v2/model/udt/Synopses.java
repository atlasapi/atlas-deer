package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "synopses")
public class Synopses {

    @Field(name = "s")
    private String shortDescr;

    @Field(name = "m")
    private String mediumDescr;

    @Field(name = "l")
    private String longDescr;

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
}
