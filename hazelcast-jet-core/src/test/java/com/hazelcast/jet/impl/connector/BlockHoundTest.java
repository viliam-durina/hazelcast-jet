package com.hazelcast.jet.impl.connector;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.blockhound.integration.BlockHoundIntegration;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BlockHoundTest {

    public static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            return new Thread(r, "my-pool-" + count.getAndIncrement());
        }
    };

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
            Executors.newSingleThreadExecutor(THREAD_FACTORY).execute(task);

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
            Executors.newSingleThreadExecutor(THREAD_FACTORY).execute(task);

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
                Executors.newSingleThreadExecutor(THREAD_FACTORY).execute(task);
                Thread.sleep(1000);
            }

            task.get(10, SECONDS);
            Assert.fail("should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue("detected", e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    public void test_contendedLogToDisabledLogger() throws Exception {
        ILogger logger = Logger.getLogger(BlockHoundTest.class);

        Runnable task = () -> {
            for (int i = 0; i < 1_000_000; i++) {
                logger.finest("foo");
            }
        };
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            threads.add(new Thread(task, "my-pool-" + i));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void test_contendedChmPutGet() throws Exception {
        ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();
        Runnable taskPut = () -> {
            for (int i = 0; i < 1_000_000; i++) {
                map.put(i, i);
            }
        };
        Runnable taskGet = () -> {
            for (int i = 0; i < 1_000_000; i++) {
                map.get(i);
            }
        };
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            threads.add(new Thread(taskPut, "my-pool-put-" + i));
            threads.add(new Thread(taskGet, "my-pool-get-" + i));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void test_contendedChmGet() throws Exception {
        ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            map.put(i, i);
        }
        Runnable taskGet = () -> {
            for (int i = 0; i < 1_000_000; i++) {
                map.get(i);
            }
        };
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            threads.add(new Thread(taskGet, "my-pool-get-" + i));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
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
