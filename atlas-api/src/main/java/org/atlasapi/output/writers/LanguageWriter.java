package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public final class LanguageWriter implements EntityListWriter<String> {

    private final Map<String, Locale> localeMap;

    public LanguageWriter(Map<String, Locale> localeMap) {
        this.localeMap = localeMap;
    }

    @Override
    public void write(String languageCode, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        Locale locale = localeMap.get(languageCode);
        if (locale != null) {
            writer.writeField("code", locale.getLanguage());
            writer.writeField("display", locale.getDisplayLanguage());
        }
    }

    @Override
    public String listName() {
        return "languages";
    }

    @Override
    public String fieldName(String entity) {
        return "language";
    }
}