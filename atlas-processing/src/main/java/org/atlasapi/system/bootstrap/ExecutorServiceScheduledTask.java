package org.atlasapi.system.bootstrap;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.metabroadcast.common.scheduling.FailedRunHandler;
import com.metabroadcast.common.scheduling.FailedRunHandlers;
import com.metabroadcast.common.scheduling.Reducible;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.scheduling.UpdateProgress.FAILURE;
import static com.metabroadcast.common.scheduling.UpdateProgress.START;
import static com.metabroadcast.common.scheduling.UpdateProgress.SUCCESS;

/**
 * Task to copy schedules from a schedule resolver to a schedule store.
 */
public final class ExecutorServiceScheduledTask<R extends Reducible<R>> extends ScheduledTask {

    private static final String TASK_TIMEOUT_FAILED = "Task failed to finish within timeout.";
    private static final String TASK_SUBMIT_FAILED_MSG = "Failed to submit all tasks.";

    private final ListeningExecutorService executor;
    private final Supplier<? extends Iterable<? extends Callable<R>>> taskSupplier;
    private final int parellelism;
    private final long timeout;
    private final TimeUnit timeoutUnit;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private AtomicReference<UpdateProgress> tasksProgress;
    private AtomicReference<R> items;
    private volatile boolean submissionFailed;
    private AtomicReference<String> msg = new AtomicReference<String>("");

    public ExecutorServiceScheduledTask(ExecutorService executor,
            Supplier<? extends Iterable<? extends Callable<R>>> taskSupplier, int parallellism,
            long timeout, TimeUnit timeoutUnit) {
        this(
                executor,
                taskSupplier,
                parallellism,
                timeout,
                timeoutUnit,
                FailedRunHandlers.logFailedRun()
        );
    }

    public ExecutorServiceScheduledTask(ExecutorService executor,
            Supplier<? extends Iterable<? extends Callable<R>>> taskSupplier, int parallellism,
            long timeout, TimeUnit timeoutUnit, FailedRunHandler failedRunHandler) {
        super(failedRunHandler);
        this.executor = MoreExecutors.listeningDecorator(executor);
        this.taskSupplier = checkNotNull(taskSupplier);
        this.parellelism = parallellism;
        this.timeout = timeout;
        this.timeoutUnit = checkNotNull(timeoutUnit);
    }

    @Override
    protected void runTask() {

        final Semaphore s = new Semaphore(parellelism);

        List<ListenableFuture<R>> submitted = Lists.newArrayList();
        tasksProgress = new AtomicReference<>(START);
        items = new AtomicReference<>();
        submissionFailed = false;

        msg.set("Submitting tasks");
        Iterator<? extends Callable<R>> tasks = taskSupplier.get().iterator();
        try {
            while (shouldContinue() && tasks.hasNext()) {
                final Callable<R> task = tasks.next();
                if (!tryGetPermit(s, 3)) {
                    msg.set("Failed to submit all tasks");
                    submissionFailed = true;
                    break;
                }
                ListenableFuture<R> result = executor.submit(task);
                submitted.add(result);
                Futures.addCallback(result, new FutureCallback<R>() {

                    @Override
                    public void onSuccess(R result) {
                        add(tasksProgress, SUCCESS);
                        add(items, result);
                        s.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        add(tasksProgress, FAILURE);
                        if (t instanceof CancellationException) {
                            log.debug("Task " + task.toString() + " cancelled");
                        } else {
                            log.warn("Task " + task.toString() + " failed", t);
                        }
                        s.release();
                    }
                });
            }

            if (submissionFailed) {
                msg.set(TASK_SUBMIT_FAILED_MSG);
                if (!waitForFinish(s)) {
                    cancel(submitted);
                    msg.set(TASK_SUBMIT_FAILED_MSG + " " + TASK_TIMEOUT_FAILED);
                }
                throw new RuntimeException(TASK_SUBMIT_FAILED_MSG);
            }

            msg.set(shouldContinue() ? "Waiting for finish" : "Aborted, waiting for finish");
            if (!waitForFinish(s)) {
                cancel(submitted);
                msg.set(TASK_TIMEOUT_FAILED);
                throw new RuntimeException(TASK_TIMEOUT_FAILED);
            }

        } catch (InterruptedException ie) {
            cancel(submitted);
            throw new RuntimeException(ie);
        }
    }

    private boolean waitForFinish(final Semaphore s)
            throws InterruptedException {
        int attempts = 0;
        while (attempts < parellelism) {
            if (s.tryAcquire(parellelism, timeout, timeoutUnit)) {
                return true;
            }
            attempts++;
        }
        return false;
    }

    private void cancel(List<ListenableFuture<R>> submitted) {
        Futures.allAsList(submitted).cancel(true);
    }

    private <T extends Reducible<T>> void add(AtomicReference<T> ref, T add) {
        T curr;
        T updated;
        do {
            curr = ref.get();
            updated = curr == null ? add : curr.reduce(add);
        } while (!ref.compareAndSet(curr, updated));
    }

    private boolean tryGetPermit(Semaphore s, int maxAttempts) throws InterruptedException {
        int attempts = 0;
        while (attempts < maxAttempts) {
            if (s.tryAcquire(timeout, timeoutUnit)) {
                return true;
            }
            attempts++;
        }
        return false;
    }

    @Override
    public String getCurrentStatusMessage() {
        return String.format(
                "%s%s%s",
                msg.get(),
                progress(" Tasks: ", tasksProgress),
                progress(" Items: ", items)
        );
    }

    private <T extends Reducible<T>> String progress(String prefix, AtomicReference<T> reducible) {
        return reducible != null ? prefix + reducible.toString() : "";
    }

}
