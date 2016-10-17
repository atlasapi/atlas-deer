package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Language;

public class LanguageSerialization {

    public Language serialize(org.atlasapi.entity.Language input) {
        Language language = new Language();
        language.setDubbing(input.getDubbing());
        language.setDisplay(input.getDisplay());
        language.setCode(input.getCode());
        return language;
    }

    public org.atlasapi.entity.Language deserialize(Language input) {
        org.atlasapi.entity.Language language =
                new org.atlasapi.entity.Language();
        language.setDisplay(input.getDisplay());
        language.setDubbing(input.getDubbing());
        language.setCode(input.getCode());
        return language;
    }
}
