package org.atlasapi.meta.annotations;

public final class IdField implements Field {

    @Override
    public final String name() {
        return "id";
    }

    @Override
    public final String value() {
        return "java.lang.String";
    }



}
