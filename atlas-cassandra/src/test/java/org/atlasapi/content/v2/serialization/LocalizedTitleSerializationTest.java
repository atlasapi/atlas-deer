package org.atlasapi.content.v2.serialization;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.atlasapi.content.v2.model.udt.LocalizedTitle;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class LocalizedTitleSerializationTest {

    private LocalizedTitleSerialization serializer;

    public LocalizedTitleSerializationTest() {
        serializer = new LocalizedTitleSerialization();
    }

    @Test
    public void localizedTitleSerializesAndDeserializes() {
        org.atlasapi.entity.LocalizedTitle localizedTitle =
                new org.atlasapi.entity.LocalizedTitle();
        localizedTitle.setType("type");
        localizedTitle.setLocale(new Locale("usa"));
        localizedTitle.setTitle("title");

        Set<org.atlasapi.entity.LocalizedTitle> localizedTitleSet = new HashSet<>();
        localizedTitleSet.add(localizedTitle);

        Set<LocalizedTitle> serializedLtSet = serializer.serialize(localizedTitleSet);

        for (LocalizedTitle lTitle : serializedLtSet) {
            assertEquals(lTitle.getType(), localizedTitle.getType());
            assertEquals(lTitle.getLocale(), localizedTitle.getLocale());
            assertEquals(lTitle.getTitle(), localizedTitle.getTitle());
        }

        Set<org.atlasapi.entity.LocalizedTitle> deserializedLtSet =
                serializer.deserialize(serializedLtSet);

        for (org.atlasapi.entity.LocalizedTitle lTitle : deserializedLtSet) {
            assertEquals(lTitle.getType(), localizedTitle.getType());
            assertEquals(lTitle.getLocale(), localizedTitle.getLocale());
            assertEquals(lTitle.getTitle(), localizedTitle.getTitle());
        }
    }
}