package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

public class License {

    private String text;

    public License(String text) {
        this.text = checkNotNull(text);
    }

    public String getText() {
        return text;
    }
}
