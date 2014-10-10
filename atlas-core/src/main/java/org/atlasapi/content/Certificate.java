package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.meta.annotations.FieldName;

import com.google.common.base.Objects;
import com.metabroadcast.common.intl.Country;

public class Certificate {

    //TODO make some static final instantiations?
    
    private final Country country;
    private final String classification;

    public Certificate(String classification, Country country) {
        this.classification = checkNotNull(classification);
        this.country = checkNotNull(country);
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
