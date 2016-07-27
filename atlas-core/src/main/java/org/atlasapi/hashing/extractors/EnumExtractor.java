package org.atlasapi.hashing.extractors;

public class EnumExtractor extends Extractor {

    private EnumExtractor() {
    }

    public static EnumExtractor create() {
        return new EnumExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object instanceof Enum;
    }

    @Override
    protected String extractValueInternal(Object object) {
        return ((Enum) object).name().toLowerCase();
    }
}
