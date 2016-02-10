package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;

public class MetaApiLinkCreator implements LinkCreator {

    private final String atlasUriBase;

    public MetaApiLinkCreator(String atlasUriBase) {
        this.atlasUriBase = checkNotNull(atlasUriBase);
    }

    @Override
    public String createModelLink(String type) {
        StringBuilder modelLink = new StringBuilder();

        modelLink.append(atlasUriBase);
        // TODO use a constant from the controller defining root path
        modelLink.append("/4/meta/types/");
        modelLink.append(type);
        modelLink.append(".json");

        return modelLink.toString();
    }
}
