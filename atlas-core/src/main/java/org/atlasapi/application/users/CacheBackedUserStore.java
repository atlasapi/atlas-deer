package org.atlasapi.application.users;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;

import com.metabroadcast.common.social.model.UserRef;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class CacheBackedUserStore implements UserStore {

    private final UserStore delegate;
    private LoadingCache<UserRef, Optional<User>> userRefCache;
    private LoadingCache<Id, Optional<User>> idCache;
    private LoadingCache<String, Set<User>> emailCache;

    public CacheBackedUserStore(final UserStore delegate) {
        this.delegate = delegate;
        this.userRefCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new CacheLoader<UserRef, Optional<User>>() {

                    @Override
                    public Optional<User> load(UserRef key) throws Exception {
                        return delegate.userForRef(key);
                    }
                });
        this.idCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new CacheLoader<Id, Optional<User>>() {

                    @Override
                    public Optional<User> load(Id key) throws Exception {
                        return delegate.userForId(key);
                    }
                });
        this.emailCache = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Set<User>>() {

                    @Override
                    public Set<User> load(String key) throws Exception {
                        return delegate.userAccountsForEmail(key);
                    }
                });
    }

    @Override
    public Optional<User> userForRef(UserRef ref) {
        return userRefCache.getUnchecked(ref);
    }

    @Override
    public Optional<User> userForId(Id id) {
        return idCache.getUnchecked(id);
    }

    @Override
    public Set<User> userAccountsForEmail(String email) {
        return emailCache.getUnchecked(email);
    }

    @Override
    public void store(User user) {
        delegate.store(user);
        userRefCache.invalidate(user.getUserRef());
        idCache.invalidate(user.getId());
        emailCache.invalidate(user.getEmail());
    }

    @Override
    public Iterable<User> allUsers() {
        return delegate.allUsers();
    }

    @Override
    public ListenableFuture<Resolved<User>> resolveIds(Iterable<Id> ids) {
        try {
            ImmutableMap<Id, Optional<User>> userIndex = idCache.getAll(ids);
            Iterable<User> users = Optional.presentInstances(userIndex.values());
            return Futures.immediateFuture(Resolved.valueOf(users));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
