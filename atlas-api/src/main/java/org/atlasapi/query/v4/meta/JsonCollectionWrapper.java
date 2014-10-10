package org.atlasapi.query.v4.meta;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;


public class JsonCollectionWrapper<T> {

    private final String name;
    private final Iterable<T> collection;
    
    public JsonCollectionWrapper(String name, Iterable<T> collection) {
        this.name = checkNotNull(name);
        this.collection = checkNotNull(collection);
    }
    
    public String name() {
        return name;
    }
    
    public Iterable<T> collection() {
        return collection;
    }
    
    public String toString() {        
        return Objects.toStringHelper(getClass())
                .add("name", name)
                .add("collection", collection.toString())
                .toString();
    }
}
