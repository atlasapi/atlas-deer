package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.KeyPhrase;

public class KeyPhraseSerialization {

    public KeyPhrase serialize(org.atlasapi.content.KeyPhrase keyPhrase) {
        if (keyPhrase == null) {
            return null;
        }

        KeyPhrase internal = new KeyPhrase();

        internal.setPhrase(keyPhrase.getPhrase());
        internal.setWeighting(keyPhrase.getWeighting());

        return internal;
    }

    public org.atlasapi.content.KeyPhrase deserialize(KeyPhrase kp) {
        if (kp == null) {
            return null;
        }

        return new org.atlasapi.content.KeyPhrase(kp.getPhrase(), kp.getWeighting());
    }
}