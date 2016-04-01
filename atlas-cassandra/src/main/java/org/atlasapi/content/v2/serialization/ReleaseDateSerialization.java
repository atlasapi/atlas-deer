package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.ReleaseDate;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

public class ReleaseDateSerialization {

    public ReleaseDate serialize(org.atlasapi.content.ReleaseDate releaseDate) {
        if (releaseDate == null) {
            return null;
        }

        ReleaseDate internal =
                new ReleaseDate();

        Country country = releaseDate.country();
        if (country != null) {
            internal.setCountry(country.code());
        }

        org.atlasapi.content.ReleaseDate.ReleaseType type = releaseDate.type();
        if (type != null) {
            internal.setType(type.name());
        }

        internal.setReleaseDate(releaseDate.date());

        return internal;
    }

    public org.atlasapi.content.ReleaseDate deserialize(ReleaseDate internal) {
        if (internal == null) {
            return null;
        }
        return new org.atlasapi.content.ReleaseDate(
                internal.getReleaseDate(),
                Countries.fromCode(internal.getCountry()),
                org.atlasapi.content.ReleaseDate.ReleaseType.valueOf(internal.getType())
        );
    }
}