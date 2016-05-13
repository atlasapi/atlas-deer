package org.atlasapi.application;

import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class CacheBackedApplicationStore implements ApplicationStore, LegacyApplicationStore {

    private final ApplicationStore delegate;
    private LoadingCache<Id, Optional<Application>> idCache;
    private LoadingCache<String, Optional<Application>> apiKeyCache;
    private LoadingCache<String, Optional<Id>> slugCache;

    public CacheBackedApplicationStore(final LegacyApplicationStore delegate, int timeoutMinutes) {
        this.delegate = delegate;
        this.idCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeoutMinutes, TimeUnit.MINUTES)
                .build(new CacheLoader<Id, Optional<Application>>() {

                    @Override
                    public Optional<Application> load(Id id) throws Exception {
                        return delegate.applicationFor(id);
                    }
                });
        this.apiKeyCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeoutMinutes, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Optional<Application>>() {

                    @Override
                    public Optional<Application> load(String key) throws Exception {
                        return delegate.applicationForKey(key);
                    }
                });
        this.slugCache = CacheBuilder.newBuilder()
                .expireAfterWrite(timeoutMinutes, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Optional<Id>>() {

                    @Override
                    public Optional<Id> load(String key) throws Exception {
                        return delegate.applicationIdForSlug(key);
                    }

                });
    }

    @Override
    public ListenableFuture<Resolved<Application>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(Iterables.transform(
                ids,
                input -> idCache.getUnchecked(input).get()
        )));
    }

    @Override
    public Iterable<Application> allApplications() {
        return delegate.allApplications();
    }

    @Override
    public Optional<Application> applicationFor(Id id) {
        return idCache.getUnchecked(id);
    }

    @Override
    public Application createApplication(Application application) {
        return delegate.createApplication(application);
    }

    @Override
    public Application updateApplication(Application application) {
        idCache.invalidate(application.getId());
        apiKeyCache.invalidate(application.getCredentials().getApiKey());
        return delegate.updateApplication(application);
    }

    @Override
    public Iterable<Application> readersFor(Publisher source) {
        return delegate.readersFor(source);
    }

    @Override
    public Iterable<Application> writersFor(Publisher source) {
        return delegate.writersFor(source);
    }

    @Override
    public Optional<Application> applicationForKey(String apiKey) {
        return apiKeyCache.getUnchecked(apiKey);
    }

    @Override
    @Deprecated
    public Optional<Id> applicationIdForSlug(String slug) {
        return slugCache.getUnchecked(slug);
    }

    @Override
    @Deprecated
    public Iterable<Id> applicationIdsForSlugs(Iterable<String> slugs) {
        return Iterables.transform(slugs, input -> slugCache.getUnchecked(input).get());
    }
}
