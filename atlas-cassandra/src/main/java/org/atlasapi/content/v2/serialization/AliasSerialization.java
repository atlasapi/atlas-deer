package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Alias;

public class AliasSerialization {

    public Alias serialize(org.atlasapi.entity.Alias alias) {
        if (alias == null) {
            return null;
        }
        Alias internal = new Alias();

        internal.setValue(alias.getValue());
        internal.setNamespace(alias.getNamespace());

        return internal;
    }

    public org.atlasapi.entity.Alias deserialize(Alias alias) {
        return new org.atlasapi.entity.Alias(alias.getNamespace(), alias.getValue());
    }
}
