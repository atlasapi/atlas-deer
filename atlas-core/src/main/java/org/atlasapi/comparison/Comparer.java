package org.atlasapi.comparison;

public abstract class Comparer {

    protected abstract boolean isSupported(Object object);

    public boolean isSupported(Object object1, Object object2) {
        return isSupported(object1) && isSupported(object2);
    }

    public abstract boolean equals(Object object1, Object object2);

}
