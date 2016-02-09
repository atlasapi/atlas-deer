package org.atlasapi.output.annotation;

import org.atlasapi.content.Encoding;
import org.atlasapi.content.Location;

import static com.google.common.base.Preconditions.checkNotNull;

public class EncodedLocation {

    private final Encoding encoding;
    private final Location location;

    public EncodedLocation(Encoding encoding, Location location) {
        this.encoding = checkNotNull(encoding);
        this.location = checkNotNull(location);
    }

    public Encoding getEncoding() {
        return this.encoding;
    }

    public Location getLocation() {
        return this.location;
    }

}
