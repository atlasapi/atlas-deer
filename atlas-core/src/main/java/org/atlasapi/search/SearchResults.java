package org.atlasapi.search;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.entity.Id;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class SearchResults implements Iterable<Id> {

    private ImmutableSet<Id> results;

    public SearchResults() { /* for GSON */ }

    public static final SearchResults from(Iterable<Long> ids) {
        return new SearchResults(Iterables.transform(ids, Id.fromLongValue()));
    }

    public SearchResults(Iterable<Id> ids) {
        //ImmutableSet maintains insertion order
        this.results = ImmutableSet.copyOf(ids);
    }

    public List<Id> getIds() {
        return results.asList();
    }

    @Override
    public Iterator<Id> iterator() {
        return results.iterator();
    }
}
