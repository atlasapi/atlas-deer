package org.atlasapi.locks;

import java.util.List;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p> Lock multiple values simultaneously. This intended for use for controlling access by a number
 * of threads to a number of resources, identified by values of type {@code T}. </p>
 * <p>
 * <p> This is not re-entrant: if the same thread attempts to lock the same value(s) twice it will
 * block until interrupted. </p>
 *
 * @param <T> - the type on which the lock acts.
 */
public final class GroupLock<T> {

    private final Histogram idsLocked;
    private final Counter threadsLocked;


    /**
     * Creates a new lock which uses the natural ordering of the value type to determine the order
     * in which locks on the value are acquired.
     *
     * @return a new GroupLock
     */
    public static <C extends Comparable<? super C>> GroupLock<C> natural(
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        return new GroupLock<C>(
                Ordering.natural(),
                metricRegistry,
                metricPrefix
        );
    }

    private final Set<T> locked = Sets.newHashSet();
    private final Logger log = LoggerFactory.getLogger(GroupLock.class);
    private final Ordering<? super T> ordering;

    private GroupLock(
            Ordering<? super T> ordering,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        this.ordering = ordering;

        this.idsLocked = metricRegistry.histogram(checkNotNull(metricPrefix) + "histogram.idsLocked");
        this.threadsLocked = metricRegistry.counter(checkNotNull(metricPrefix) + "counter.threadsLocked");

    }

    /**
     * <p> Lock the provided {@code id} so no other thread may lock it. If the {@code id} is already
     * locked then this thread blocks until awoken. </p>
     * <p>
     * <p> Whether or not this value has already been locked is determined by equality. </p>
     *
     * @param id - the value on which to lock.
     * @throws InterruptedException thread was interrupted whilst waiting for the lock.
     */
    public void lock(T id) throws InterruptedException {
        threadsLocked.inc();
        try {
            idsLocked.update(1);
            lockInternal(id);
        } finally {
            threadsLocked.dec();
        }
    }

    /**
     * <p> Release the lock held on {@code id}. Any threads also attempting to lock this value will
     * be awoken. </p>
     * <p>
     * <p> If the value is not locked then this call has no effect. </p>
     *
     * @param id - the id to unlock
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
     * @param id - the value to attempt to lock
     * @return true if the value was successfully locked, false otherwise.
     * @throws InterruptedException thread was interrupted whilst waiting for the lock.
     */
    public boolean tryLock(T id) throws InterruptedException {
        threadsLocked.inc();
        try {
            idsLocked.update(1);
            return tryLockInternal(id);
        } finally {
            threadsLocked.dec();
        }
    }

    /**
     * <p> Acquires locks on a number of ids progressively, in order specified by this locks
     * ordering. </p>
     * <p>
     * <p> E.g. if {@code lock(Arrays.asList("A","B","C"))} is called by Thread 1 whilst {@code "C"}
     * is already locked by Thread 2 then Thread 1 blocks, holding locks on {@code "A"} and {@code
     * "B"}, until it can lock {@code "C"}. </p>
     *
     * @param ids - the values to lock
     * @throws InterruptedException thread was interrupted whilst waiting for the lock.
     */
    public void lock(Set<T> ids) throws InterruptedException {
        threadsLocked.inc();
        try {
            idsLocked.update(ids.size());
            for (T id : ordering.sortedCopy(ids)) {
                lockInternal(id);
            }
        } finally {
            threadsLocked.dec();
        }
    }

    /**
     * Release locks on all the ids, if any were held.
     *
     * @param ids = the values to unlock
     */
    public void unlock(Set<T> ids) {
        for (T id : ordering.sortedCopy(ids)) {
            unlock(id);
        }
    }

    /**
     * Attempt to lock all the ids. If not all can be locked then none are.
     *
     * @param ids - the values to attempt to lock
     * @return true if all ids were locked, false otherwise.
     * @throws InterruptedException thread was interrupted whilst waiting for the lock.
     */
    public boolean tryLock(Set<T> ids) throws InterruptedException {
        threadsLocked.inc();
        try {
            idsLocked.update(ids.size());
            List<T> orderedIds = ordering.sortedCopy(ids);
            for (T id : orderedIds) {
                if(!tryLockInternal(id)) {
                    unlockTill(orderedIds, id);
                    return false;
                }
            }
            return true;
        } finally {
            threadsLocked.dec();
        }
    }

    private void lockInternal(T id) throws InterruptedException {
        log.trace("{} trying to lock {}", Thread.currentThread().getName(), id.toString());
        synchronized (locked) {
            while (locked.contains(id)) {
                log.trace(
                        "{} waiting on lock for {}",
                        Thread.currentThread().getName(),
                        id.toString()
                );
                locked.wait();
            }
            log.trace("{} acquired lock for {}", Thread.currentThread().getName(), id.toString());
            locked.add(id);
        }
    }

    private boolean tryLockInternal(T id) throws InterruptedException {
        log.trace("{} trying to lock {}", Thread.currentThread().getName(), id.toString());
        synchronized (locked) {
            if(!locked.contains(id)) {
                locked.add(id);
                return true;
            } else {
                log.trace("{} didnt get lock {}", Thread.currentThread().getName(), id.toString());
                return false;
            }
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
