package org.atlasapi.hashing.extractors;

import org.atlasapi.application.v3.SourceStatus;

public class SourceStatusExtractor extends Extractor {

    private SourceStatusExtractor() {
    }

    public static SourceStatusExtractor create() {
        return new SourceStatusExtractor();
    }

    @Override
    protected boolean isSupported(Object object) {
        return object instanceof SourceStatus;
    }

    @Override
    protected String extractValueInternal(Object object) {
        SourceStatus sourceStatus = (SourceStatus) object;
        return sourceStatus.getState().name() + sourceStatus.isEnabled();
    }
}
