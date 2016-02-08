package org.atlasapi.query.annotation;

import java.util.List;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.query.common.Resource;

import static com.google.common.base.Preconditions.checkNotNull;

final class PathAnnotation {

    private final List<Resource> path;
    private final Annotation annotation;

    public PathAnnotation(List<Resource> path, Annotation annotation) {
        this.path = checkNotNull(path);
        this.annotation = checkNotNull(annotation);
    }

    public List<Resource> getPath() {
        return path;
    }

    public Annotation getAnnotation() {
        return annotation;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof PathAnnotation) {
            PathAnnotation other = (PathAnnotation) that;
            return path.equals(other.path)
                    && annotation.equals(other.annotation);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return path.hashCode() ^ annotation.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", path, annotation);
    }

}