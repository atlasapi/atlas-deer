package org.atlasapi.output;

public class Resolved<R> {

    private R type;

    protected Resolved(R type) {
        this.type = type;
    }

     R get() {
        return type;
    }
}
