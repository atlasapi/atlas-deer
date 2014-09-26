package org.atlasapi.meta.annotations;

public final class DescriptionField implements Field {

    @Override
    public final String name() {
        return "description";
    }

    @Override
    public final String value() {
        return "java.lang.String";
    }



}
