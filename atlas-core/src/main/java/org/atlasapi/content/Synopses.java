package org.atlasapi.content;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

public class Synopses implements Hashable {

    private String shortDescription;
    private String mediumDescription;
    private String longDescription;

    public static Synopses withShortDescription(String shortDescription) {
        Synopses synopses = new Synopses();
        synopses.setShortDescription(shortDescription);
        return synopses;
    }

    public static Synopses withMediumDescription(String mediumDescription) {
        Synopses synopses = new Synopses();
        synopses.setMediumDescription(mediumDescription);
        return synopses;
    }

    public static Synopses withLongDescription(String longDescription) {
        Synopses synopses = new Synopses();
        synopses.setLongDescription(longDescription);
        return synopses;
    }

    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public void setMediumDescription(String mediumDescription) {
        this.mediumDescription = mediumDescription;
    }

    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

    @FieldName("short_description")
    public String getShortDescription() {
        return shortDescription;
    }

    @FieldName("medium_description")
    public String getMediumDescription() {
        return mediumDescription;
    }

    @FieldName("long_description")
    public String getLongDescription() {
        return longDescription;
    }
}
