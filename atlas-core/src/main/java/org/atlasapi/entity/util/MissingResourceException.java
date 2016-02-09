package org.atlasapi.entity.util;

import org.atlasapi.entity.Id;

public class MissingResourceException extends WriteException {

    private Id missingId;

    public MissingResourceException(Id missingId) {
        this.missingId = missingId;
    }

    public Id getMissingId() {
        return missingId;
    }

    @Override
    public String getMessage() {
        return "missing " + getMissingId().toString();
    }

}
