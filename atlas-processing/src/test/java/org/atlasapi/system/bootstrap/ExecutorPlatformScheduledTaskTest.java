package org.atlasapi.system.bootstrap;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.metabroadcast.common.scheduling.FailedRunHandler;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.ScheduledTask.TaskState;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ExecutorPlatformScheduledTaskTest {

    @Test
    public void testTasksRunInParallel() {

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean task1ran = new AtomicBoolean(false);
        final AtomicBoolean task2ran = new AtomicBoolean(false);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
                supply(ImmutableList.of(new Callable<UpdateProgress>() {

                                            @Override
                                            public UpdateProgress call() throws Exception {
                                                barrier.await();
                                                task1ran.set(true);
                                                return UpdateProgress.START;
                                            }
                                        }, new Callable<UpdateProgress>() {

                                            @Override
                                            public UpdateProgress call() throws Exception {
                                                barrier.await();
                                                task2ran.set(true);
                                                return UpdateProgress.START;
                                            }
                                        }
                ));
        ExecutorServiceScheduledTask<UpdateProgress> task
                = new ExecutorServiceScheduledTask<UpdateProgress>(
                executor,
                taskSupplier,
                2,
                1,
                TimeUnit.SECONDS
        );

        task.run();

        assertTrue(task1ran.get());
        assertTrue(task2ran.get());
    }

    @Test
    public void testFailsOnTimeout() throws InterruptedException {

        final CountDownLatch taskFinish = new CountDownLatch(1);
        final AtomicBoolean taskInterrupted = new AtomicBoolean(false);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
                supply(ImmutableList.of(new Callable<UpdateProgress>() {

                                            @Override
                                            public UpdateProgress call() throws Exception {
                                                try {
                                                    Thread.sleep(500);
                                                } catch (InterruptedException ie) {
                                                    taskInterrupted.set(true);
                                                }
                                                taskFinish.countDown();
                                                return UpdateProgress.START;
                                            }
                                        }
                ));
        ExecutorServiceScheduledTask<UpdateProgress> task
                = new ExecutorServiceScheduledTask<UpdateProgress>(executor,
                taskSupplier,
                1,
                250,
                TimeUnit.MILLISECONDS,
                new FailedRunHandler() {

                    @Override
                    public void handle(ScheduledTask task, Throwable e) {
                        assertTrue(e instanceof RuntimeException);
                    }
                }
        );

        task.run();
        assertTrue("Task didn't finish", taskFinish.await(10, TimeUnit.SECONDS));
        assertTrue("Task wasn't interrupted", taskInterrupted.get());
        assertTrue("Task wasn't marked as failed", task.getState().equals(TaskState.FAILED));
    }

    @Test
    public void testFailsAfterThreeAttemptsToSubmitJobAndRunningTasksInterrupted()
            throws InterruptedException {

        final AtomicBoolean taskInterrupted = new AtomicBoolean(false);
        final AtomicBoolean neverRun = new AtomicBoolean(true);
        final CountDownLatch firstTaskFinish = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
                supply(ImmutableList.of(new Callable<UpdateProgress>() {

                    @Override
                    public UpdateProgress call() throws Exception {
                        try {
                            Thread.sleep(1300);
                        } catch (InterruptedException ie) {
                            taskInterrupted.set(true);
                        }
                        firstTaskFinish.countDown();
                        return UpdateProgress.START;
                    }
                }, new Callable<UpdateProgress>() {

                    @Override
                    public UpdateProgress call() throws Exception {
                        neverRun.set(false);
                        return UpdateProgress.START;
                    }
                }));
        ExecutorServiceScheduledTask<UpdateProgress> task
                = new ExecutorServiceScheduledTask<UpdateProgress>(executor,
                taskSupplier,
                1,
                250,
                TimeUnit.MILLISECONDS,
                new FailedRunHandler() {

                    @Override
                    public void handle(ScheduledTask task, Throwable e) {
                        assertTrue(e instanceof RuntimeException);
                    }
                }
        );

        task.run();

        assertTrue("First task didn't finish", firstTaskFinish.await(10, TimeUnit.SECONDS));
        assertTrue("Second task ran unexpectedly", neverRun.get());
        assertTrue("First task was not interrupted", taskInterrupted.get());
        assertTrue("Task wasn't marked as failed", task.getState().equals(TaskState.FAILED));
    }

    @Test
    public void testFailsAfterThreeAttemptsToSubmitJobAndMarkedAsFailed()
            throws InterruptedException {

        final AtomicBoolean neverRun = new AtomicBoolean(true);
        final CountDownLatch firstTaskFinish = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
                supply(ImmutableList.of(new Callable<UpdateProgress>() {

                    @Override
                    public UpdateProgress call() throws Exception {
                        //After three attempts but before final timeout.
                        Thread.sleep(900);
                        firstTaskFinish.countDown();
                        return UpdateProgress.START;
                    }
                }, new Callable<UpdateProgress>() {

                    @Override
                    public UpdateProgress call() throws Exception {
                        neverRun.set(false);
                        return UpdateProgress.START;
                    }
                }));
        ExecutorServiceScheduledTask<UpdateProgress> task
                = new ExecutorServiceScheduledTask<UpdateProgress>(executor,
                taskSupplier,
                1,
                250,
                TimeUnit.MILLISECONDS,
                new FailedRunHandler() {

                    @Override
                    public void handle(ScheduledTask task, Throwable e) {
                        assertTrue(e instanceof RuntimeException);
                    }
                }
        );

        task.run();

        assertTrue("First task didn't finish", firstTaskFinish.await(10, TimeUnit.SECONDS));
        assertTrue("Second task ran unexpectedly", neverRun.get());
        assertTrue("Task wasn't marked as failed", task.getState().equals(TaskState.FAILED));
    }

    private Supplier<Iterable<? extends Callable<UpdateProgress>>> supply(
            final Iterable<? extends Callable<UpdateProgress>> tasks) {
        return new Supplier<Iterable<? extends Callable<UpdateProgress>>>() {

            @Override
            public Iterable<? extends Callable<UpdateProgress>> get() {
                return tasks;
            }
        };
    }

}
