package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "keyphrase")
public class KeyPhrase {

    @Field(name = "phrase") private String phrase;
    @Field(name = "weighting") private Double weighting;

    public KeyPhrase() {}

    public String getPhrase() {
        return phrase;
    }

    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    public Double getWeighting() {
        return weighting;
    }

    public void setWeighting(Double weighting) {
        this.weighting = weighting;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        KeyPhrase keyPhrase = (KeyPhrase) object;
        return Objects.equals(phrase, keyPhrase.phrase) &&
                Objects.equals(weighting, keyPhrase.weighting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phrase, weighting);
    }
}
