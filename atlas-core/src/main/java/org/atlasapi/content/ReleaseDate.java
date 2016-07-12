package org.atlasapi.content;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.intl.Country;

import com.google.common.base.Objects;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReleaseDate implements Hashable {

    public enum ReleaseType {
        GENERAL
    }

    private final LocalDate date;
    private final Country country;
    private final ReleaseType type;

    public ReleaseDate(LocalDate date, Country country, ReleaseType type) {
        this.date = checkNotNull(date);
        this.country = checkNotNull(country);
        this.type = checkNotNull(type);
    }

    @FieldName("country")
    public Country country() {
        return country;
    }

    @FieldName("date")
    public LocalDate date() {
        return date;
    }

    @FieldName("type")
    public ReleaseType type() {
        return type;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ReleaseDate) {
            ReleaseDate other = (ReleaseDate) that;
            return date.equals(other.date)
                    && country.equals(other.country)
                    && type.equals(other.type);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(date, country, type);
    }

    @Override
    public String toString() {
        return String.format(
                "%s (%s %s)",
                date.toString(),
                country.code(),
                type.toString().toLowerCase()
        );
    }
}
