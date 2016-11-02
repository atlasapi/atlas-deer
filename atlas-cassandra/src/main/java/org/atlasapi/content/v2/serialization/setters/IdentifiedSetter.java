package org.atlasapi.content.v2.serialization.setters;

import org.atlasapi.content.v2.model.Identified;

public class IdentifiedSetter {

    private final IdentifiedWithoutUpdateTimesSetter withoutTimes = new IdentifiedWithoutUpdateTimesSetter();
    private final WithUpdateTimesSetter times = new WithUpdateTimesSetter();

    public void serialize(Identified internal, org.atlasapi.entity.Identified content) {
        withoutTimes.serialize(internal, content);
        times.serialize(internal, content);
    }

    public void deserialize(org.atlasapi.entity.Identified content, Identified internal) {
        withoutTimes.deserialize(content, internal);
        times.deserialize(content, internal);
    }
}
