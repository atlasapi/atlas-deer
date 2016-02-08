package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;

public class KeyPhraseSerializer {

    public KeyPhrase deserialize(ContentProtos.KeyPhrase phrase) {
        return new KeyPhrase(
                phrase.getPhrase(),
                phrase.hasWeighting() ? phrase.getWeighting() : null
        );
    }

    public ContentProtos.KeyPhrase serialize(KeyPhrase keyPhrase) {
        ContentProtos.KeyPhrase.Builder phrase = ContentProtos.KeyPhrase.newBuilder()
                .setPhrase(keyPhrase.getPhrase());
        if (keyPhrase.getWeighting() != null) {
            phrase.setWeighting(keyPhrase.getWeighting());
        }
        return phrase.build();
    }
}
