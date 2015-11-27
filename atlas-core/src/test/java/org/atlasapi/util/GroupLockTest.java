package org.atlasapi.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;


public class GroupLockTest {

    @Test
    public void testCanLockDifferentThings() throws InterruptedException {
        
        final GroupLock<String> lock = GroupLock.<String>natural();
        
        lock.lock("A");

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(1);
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Callable<Void>(){
            @Override
            public Void call() throws Exception {
                start.await();
                lock.lock("B");
                finish.countDown();
                return null;
            }
        });
        start.countDown();

        assertTrue("Should be able to lock A and B", finish.await(1, TimeUnit.SECONDS));
    }
    
    @Test
    public void testCantAcquireLockForKeyTwice() throws InterruptedException {

        final GroupLock<String> lock = GroupLock.<String>natural();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);
        
        final String id = "A";
        lock.lock(id);
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                start.await();
                lock.lock(id);
                latch.countDown();
                return null;
            }
        });

        start.countDown();
        assertEquals(1, latch.getCount());
        lock.unlock(id);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }
    
    @Test
    public void testTryLock() throws InterruptedException {

        final GroupLock<String> lock = GroupLock.<String>natural();

        String id = "A";
        assertTrue(lock.tryLock(id));
        assertFalse(lock.tryLock(id));
        lock.unlock(id);
        assertTrue(lock.tryLock(id));
    }

    @Test
    public void testCantLockGroupWhereOneElementIsLocked() throws InterruptedException {

        final GroupLock<String> lock = GroupLock.<String>natural();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch latch = new CountDownLatch(1);

        final String id = "B";
        lock.lock(id);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                start.await();
                lock.lock(ImmutableSet.of("A","B","C"));
                latch.countDown();
                return null;
            }
        });

        start.countDown();
        assertEquals(1, latch.getCount());
        lock.unlock(id);
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testLocksGroupElementsInOrder() throws InterruptedException {

        final GroupLock<String> lock = GroupLock.<String>natural();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch one = new CountDownLatch(1);
        final CountDownLatch two = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(2);
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                start.await();
                lock.lock(ImmutableSet.of("A","B","C"));
                one.countDown();
                lock.unlock(ImmutableSet.of("A","B","C"));
                finish.countDown();
                return null;
            }
        });
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                start.await();
                lock.lock(ImmutableSet.of("C","B","A"));
                two.countDown();
                lock.unlock(ImmutableSet.of("C","B","A"));
                finish.countDown();
                return null;
            }
        });
        
        assertEquals(1, one.getCount());
        assertEquals(1, two.getCount());
        start.countDown();
        assertTrue(one.await(1, TimeUnit.SECONDS));
        assertTrue(two.await(1, TimeUnit.SECONDS));
        assertTrue(finish.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testUnlocksAllGroupElementsIfTryLockFailsAGroup() throws InterruptedException {

        final GroupLock<String> lock = GroupLock.<String>natural();
        lock.lock("B");
        assertFalse(lock.tryLock(ImmutableSet.of("A","B","C")));
        assertTrue(lock.tryLock("A"));
        assertTrue(lock.tryLock("C"));
        lock.unlock(ImmutableSet.of("A","B","C"));
        assertTrue(lock.tryLock(ImmutableSet.of("A","B","C")));
    }

}
