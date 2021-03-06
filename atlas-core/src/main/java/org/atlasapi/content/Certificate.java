package org.atlasapi.content;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.intl.Country;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class Certificate implements Hashable {

    private Country country;
    private String classification;

    public Certificate() {

    }

    public Certificate(String classification, Country country) {
        this.classification = checkNotNull(classification);
        this.country = checkNotNull(country);
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    @FieldName("country")
    public Country country() {
        return country;
    }

    @FieldName("classification")
    public String classification() {
        return classification;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Certificate) {
            Certificate other = (Certificate) that;
            return classification.equals(other.classification) && country.equals(other.country);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(classification, country);
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", classification, country.code());
    }
}
