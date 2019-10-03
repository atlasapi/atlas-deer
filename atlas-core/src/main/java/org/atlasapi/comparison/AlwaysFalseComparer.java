package org.atlasapi.comparison;

/**
 * Extractor for classes from which a value representing their state can be extracted by just
 * calling <code>#toString()</code>
 */
public class AlwaysFalseComparer extends Comparer {

    public AlwaysFalseComparer() {

    }

    @Override
    public boolean isSupported(Object object) {
        return true;

    }

    @Override
    public boolean equals(Object object1, Object object2) {
        return false;
    }
}
