package com.hazelcast.jet.impl.connector;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.blockhound.integration.BlockHoundIntegration;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockHoundTest {

    @BeforeClass
    public static void setUpClass() {
        BlockHound.builder()
                  .disallowBlockingCallsInside(Callable.class.getName(), "call")
                  .with(new JetBlockHoundIntegration())
                  .install();
    }

    @Test
    public void blockHoundWorks() throws TimeoutException, InterruptedException {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Executors.newSingleThreadExecutor().execute(task);

            task.get(10, TimeUnit.SECONDS);
            Assert.fail("should fail");
        } catch (ExecutionException e) {
            Assert.assertTrue("detected", e.getCause() instanceof BlockingOperationError);
        }
    }

    public static class JetBlockHoundIntegration implements BlockHoundIntegration {

        @Override
        public void applyTo(BlockHound.Builder builder) {
            builder.nonBlockingThreadPredicate(current -> {
                return current.or(t -> {
                    if (t.getName() == null) {
                        return false;
                    }
                    return t.getName().contains("my-pool-");
                });
            });
        }
    }
}
