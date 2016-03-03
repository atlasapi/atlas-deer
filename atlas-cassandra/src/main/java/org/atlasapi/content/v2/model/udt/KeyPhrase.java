package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "keyphrase")
public class KeyPhrase {

    private String phrase;
    private Double weighting;

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
}
