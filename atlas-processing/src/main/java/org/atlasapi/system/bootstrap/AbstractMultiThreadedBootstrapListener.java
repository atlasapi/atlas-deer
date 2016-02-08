package org.atlasapi.system.bootstrap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMultiThreadedBootstrapListener<T> implements BootstrapListener<T> {

    private static final int NO_KEEP_ALIVE = 0;
    private final Logger log = LoggerFactory.getLogger(AbstractMultiThreadedBootstrapListener.class);

    private final ThreadPoolExecutor executor;

    public AbstractMultiThreadedBootstrapListener(int concurrencyLevel) {
        this(new ThreadPoolExecutor(concurrencyLevel, concurrencyLevel,
                NO_KEEP_ALIVE, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(100 * Runtime.getRuntime().availableProcessors()),
                new ThreadFactoryBuilder().setNameFormat(AbstractMultiThreadedBootstrapListener.class
                        + " Thread %d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        ));
    }

    public AbstractMultiThreadedBootstrapListener(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void beforeChange() {
        // No-op
    }

    @Override
    public void afterChange() {
        // No-op
    }

    @Override
    public void onChange(Iterable<? extends T> changed) {
        for (final T change : changed) {
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        onChange(change);
                    } catch (Exception ex) {
                        log.warn("Failed to process content {}, exception follows.", change);
                        log.warn(ex.getMessage(), ex);
                    }
                }
            });
        }
    }

    protected abstract void onChange(T change) throws Exception;
}
