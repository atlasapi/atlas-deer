package org.atlasapi.application;

public enum UsageType {
    COMMERCIAL("Commercial"),
    NON_COMMERCIAL("Non commericial"),
    PERSONAL("Personal");

    private final String title;

    UsageType(String title) {
        this.title = title;
    }

    public String title() {
        return this.title;
    }
}
