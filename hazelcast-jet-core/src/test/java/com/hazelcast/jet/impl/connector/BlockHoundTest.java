package com.hazelcast.jet.impl.connector;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.blockhound.integration.BlockHoundIntegration;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BlockHoundTest {

    @BeforeClass
    public static void setUpClass() {
        BlockHound.builder()
                  .with(new TestBlockHoundIntegration())
                  .install();
    }

    @Test
    public void blockHoundWorks() throws Exception {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Executors.newSingleThreadExecutor(new ThreadFactory() {
                private final AtomicInteger count = new AtomicInteger();

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    return new Thread(r, "my-pool-" + count.getAndIncrement());
                }
            }).execute(task);

            task.get(10, SECONDS);
            Assert.fail("should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue("detected", e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    public void test_semaphore_tryAcquire() throws Exception {
        try {
            Semaphore s = new Semaphore(0);
            FutureTask<?> task = new FutureTask<>(() -> {
                s.tryAcquire(1, SECONDS);
                return "";
            });
            Executors.newSingleThreadExecutor(new ThreadFactory() {
                private final AtomicInteger count = new AtomicInteger();

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    return new Thread(r, "my-pool-" + count.getAndIncrement());
                }
            }).execute(task);

            task.get(10, SECONDS);
            Assert.fail("should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue("detected", e.getCause() instanceof BlockingOperationError);
            e.printStackTrace();
        }
    }

    @Test
    public void test_detectingWaitingForAMonitor() throws TimeoutException, InterruptedException {
        try {
            Object monitor = new Object();

            FutureTask<?> task = new FutureTask<>(() -> {
                try {
                    synchronized (monitor) {
                        return "";
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw e;
                }
            });
            synchronized (monitor) {
                Executors.newSingleThreadExecutor(new ThreadFactory() {
                    private final AtomicInteger count = new AtomicInteger();

                    @Override
                    public Thread newThread(@Nonnull Runnable r) {
                        return new Thread(r, "my-pool-" + count.getAndIncrement());
                    }
                }).execute(task);
                Thread.sleep(1000);
            }

            task.get(10, SECONDS);
            Assert.fail("should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue("detected", e.getCause() instanceof BlockingOperationError);
        }
    }

    public static class TestBlockHoundIntegration implements BlockHoundIntegration {

        @Override
        public void applyTo(BlockHound.Builder builder) {
            builder.nonBlockingThreadPredicate(current -> {
                return current.or(t -> {
                    if (t.getName() == null) {
                        return false;
                    }
                    return t.getName().contains("my-pool-");
                });
            })
                   .allowBlockingCallsInside("java.lang.ThreadGroup", "uncaughtException")
                   .allowBlockingCallsInside("java.util.concurrent.ThreadPoolExecutor", "getTask");
        }
    }
}
