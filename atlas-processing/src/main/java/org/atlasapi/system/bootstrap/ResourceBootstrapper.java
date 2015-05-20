package org.atlasapi.system.bootstrap;

import static com.google.common.base.Predicates.notNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.system.legacy.exception.LegacyChannelNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ResourceBootstrapper<T extends Identifiable> {

    private static final String NONE = "NONE";
    private static final String SUCCESS = "SUCCESS";
    private static final String FAIL = "FAIL";

    private static final Logger log = LoggerFactory.getLogger(ResourceBootstrapper.class);

    private final ReentrantLock bootstrapLock = new ReentrantLock();
    private final AtomicReference<String> lastStatus = new AtomicReference<String>(NONE);
    private final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
    private volatile boolean bootstrapping;
    private volatile String destination;

    private ResourceLister<T> lister;
    private final int batchSize;
    
    public ResourceBootstrapper(ResourceLister<T> lister) {
        this.lister = lister;
        batchSize = 100;
    }
    
    public boolean loadAllIntoListener(BootstrapListener<? super T> listener) {
        if (bootstrapLock.tryLock()) {
            try {
                Exception error = null;
                bootstrapping = true;
                destination = listener.getClass().toString();
                listener.beforeChange();
                try {
                    log.info("Bootstrapping: {} -> {}", destination, lister.getClass());
                    int processedResources = bootstrapResource(listener);
                    log.info("Bootstrapped: {} to {}", processedResources, destination);
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                    error = ex;
                    throw new RuntimeException(ex.getMessage(), ex);
                } finally {
                    if (error == null) {
                        lastStatus.set(SUCCESS);
                    } else {
                        lastException.set(error);
                        lastStatus.set(FAIL);
                    }
                    listener.afterChange();
                }
            } finally {
                bootstrapping = false;
                bootstrapLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }
    
    public boolean isBootstrapping() {
        return bootstrapping;
    }

    public String getDestination() {
        return destination;
    }

    public String getLastStatus() {
        return lastStatus.get();
    }
    
    private int bootstrapResource(final BootstrapListener<? super T> listener) {
        int processed = 0;
        Iterable<List<T>> partitioned = Iterables.partition(lister.list(), batchSize);
        for (List<T> partition : partitioned) {
            try {
                listener.onChange(ImmutableList.copyOf(Iterables.filter(partition, notNull())));
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
            processed += partition.size();
            log.info("Bootstrapping: {} to {}", processed, destination);
        }
        return processed;
    }

    public Exception getLastException() {
        return lastException.get();
    }
    
}
