package org.atlasapi.application;

import static com.google.common.base.Preconditions.checkNotNull;

public enum UsageType {
    COMMERCIAL("Commercial"),
    NON_COMMERCIAL("Non commericial"),
    PERSONAL("Personal");

    private final String title;

    UsageType(String title) {
        this.title = checkNotNull(title);
    }

    public String title() {
        return this.title;
    }
}
