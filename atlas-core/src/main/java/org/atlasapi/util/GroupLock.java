package org.atlasapi.util;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

/**
 * <p>
 * Lock multiple values simultaneously. This intended for use for controlling
 * access by a number of threads to a number of resources, identified by values
 * of type {@code T}.
 * </p>
 * 
 * <p>
 * This is not re-entrant: if the same thread attempts to lock the same value(s)
 * twice it will block until interrupted.
 * </p>
 * 
 * @param <T>
 *            - the type on which the lock acts.
 */
public final class GroupLock<T> {

    /**
     * Creates a new lock which uses the natural ordering of the value type to
     * determine the order in which locks on the value are acquired.
     * 
     * @return a new GroupLock
     */
    public static final <C extends Comparable<? super C>> GroupLock<C> natural() {
        return new GroupLock<C>(Ordering.natural());
    }

    /**
     * Creates a new lock which uses the ordering of the provided comparator to
     * determine the order in which locks on the value are acquired.
     * 
     * @return a new GroupLock
     */
    public static final <U> GroupLock<U> fromComparator(Comparator<? super U> comparator) {
        return new GroupLock<U>(Ordering.from(comparator));
    }

    private final Set<T> locked = Sets.newHashSet();
    private final Logger log = LoggerFactory.getLogger(GroupLock.class);
    private final Ordering<? super T> ordering;

    private GroupLock(Ordering<? super T> ordering) {
        this.ordering = ordering;
    }

    /**
     * <p>
     * Lock the provided {@code id} so no other thread may lock it. If the
     * {@code id} is already locked then this thread blocks until awoken.
     * </p>
     * 
     * <p>
     * Whether or not this value has already been locked is determined by
     * equality.
     * </p>
     * 
     * @param id
     *            - the value on which to lock.
     * @throws InterruptedException
     *             thread was interrupted whilst waiting for the lock.
     */
    public void lock(T id) throws InterruptedException {
        log.trace("{} trying to lock {}", Thread.currentThread().getName(), id.toString());
        synchronized (locked) {
            while (locked.contains(id)) {
                log.trace("{} waiting on lock for {}", Thread.currentThread().getName(), id.toString());
                locked.wait();
            }
            log.trace("{} acquired lock for {}", Thread.currentThread().getName(), id.toString());
            locked.add(id);
        }
    }

    /**
     * <p>
     * Release the lock held on {@code id}. Any threads also attempting to lock
     * this value will be awoken.
     * </p>
     * 
     * <p>
     * If the value is not locked then this call has no effect.
     * </p>
     * 
     * @param id
     *            - the id to unlock
     * @throws InterruptedException
     */
    public void unlock(T id) {
        log.trace("{} trying to unlock {}", Thread.currentThread().getName(), id.toString());
        synchronized (locked) {
            if (locked.remove(id)) {
                log.trace("{} unlocked {}", Thread.currentThread().getName(), id.toString());
                locked.notifyAll();
            }
        }
    }

    /**
     * Attempt to lock {@code id}, if it is not already locked.
     * 
     * @param id
     *            - the value to attempt to lock
     * @return true if the value was successfully locked, false otherwise.
     * @throws InterruptedException
     *             thread was interrupted whilst waiting for the lock.
     */
    public boolean tryLock(T id) throws InterruptedException {
        log.trace("{} attempting to lock {}", Thread.currentThread().getName(), id.toString());
        synchronized (locked) {
            if (!locked.contains(id)) {
                log.trace("{} got lock for {}", Thread.currentThread().getName(), id.toString());
                lock(id);
                return true;
            }
            log.trace("{} didnt get lock {}", Thread.currentThread().getName(), id.toString());
            return false;
        }
    }

    /**
     * <p>
     * Acquires locks on a number of ids progressively, in order specified by
     * this locks ordering.
     * </p>
     * 
     * <p>
     * E.g. if {@code lock(Arrays.asList("A","B","C"))} is called by Thread 1
     * whilst {@code "C"} is already locked by Thread 2 then Thread 1 blocks,
     * holding locks on {@code "A"} and {@code "B"}, until it can lock
     * {@code "C"}.
     * </p>
     * 
     * @param ids
     *            - the values to lock
     * @throws InterruptedException
     *             thread was interrupted whilst waiting for the lock.
     */
    public void lock(Set<T> ids) throws InterruptedException {
        for (T id : ordering.sortedCopy(ids)) {
            lock(id);
        }
    }

    /**
     * Release locks on all the ids, if any were held.
     * 
     * @param ids
     *            = the values to unlock
     */
    public void unlock(Set<T> ids) {
        for (T id : ordering.sortedCopy(ids)) {
            unlock(id);
        }
    }

    /**
     * Attempt to lock all the ids. If not all can be locked then none are.
     * 
     * @param ids
     *            - the values to attempt to lock
     * @return true if all ids were locked, false otherwise.
     * @throws InterruptedException
     *             thread was interrupted whilst waiting for the lock.
     */
    public boolean tryLock(Set<T> ids) throws InterruptedException {
        synchronized (locked) {
            List<T> orderedIds = ordering.sortedCopy(ids);
            for (T id : orderedIds) {
                if (!tryLock(id)) {
                    unlockTill(orderedIds, id);
                    return false;
                }
            }
            return true;
        }
    }

    private void unlockTill(List<T> orderedIds, T limit) {
        for (T id : orderedIds) {
            if (id == limit) {
                return;
            }
            unlock(id);
        }
    }

}
