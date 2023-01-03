package io.jeffrey.messaging.transactionalOutbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeStreamDataCaptureTests {

    @BeforeEach
    public void reset() {

    }

    /**
     * Base test case which demonstrate the normal flow (happy path)
     * where the input is distributed among the threads and fully
     * processed.
     */
    @Test
    @Timeout(5)
    public void test_001() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                for (int cursor=0; cursor<input.length; cursor++) {
                    if (!processed.containsKey(input[cursor])) {
                        processed.put(input[cursor], "PROCESSING");
                        System.out.println("Thread 1, processing: " + input[cursor]);
                        // commit
                        processed.put(input[cursor], "COMPLETED");
                        latch.countDown();
                    }
                }
            }
        });

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                for (int cursor=0; cursor<input.length; cursor++) {
                    if (!processed.containsKey(input[cursor])) {
                        processed.put(input[cursor], "PROCESSING");
                        System.out.println("Thread 2, processing: " + input[cursor]);
                        // commit
                        processed.put(input[cursor], "COMPLETED");
                        latch.countDown();
                    }
                }
            }
        });

        latch.await();
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }
    }

    /**
     * Introduce basic resume token mechanism.
     * Test case demonstrating thread B can resume
     * working from the point where thread A stop.
     */
    @Test
    @Timeout(5)
    public void test_002() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(1);

        final AtomicInteger resumeToken = new AtomicInteger(0);

        executorService.execute(() -> {
            for (int cursor=resumeToken.get(); cursor<input.length; cursor++) {
                if (cursor == 5) break; // simulate crash after processing 5 items
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 1, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                    resumeToken.getAndAdd(1);
                }
            }
        });

        executorService.execute(() -> {
            assertEquals(5, resumeToken.get());
            System.out.println("Thread 2 will resume at: " + resumeToken);

            for (int cursor=resumeToken.get(); cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 2, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                    resumeToken.getAndAdd(1);
                }
            }
        });

        latch.await();
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }
    }

    /**
     * Demonstrate single thread crash and
     * resumption from previous crash point
     * by newly spawned thread
     */
    @Test
    @Timeout(5)
    public void test_003() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(1);

        // introduce workers map to keep track of resume token timestamp
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.execute(() -> {
            int resumeToken = 0;
            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (cursor == 3) break; // simulate crash after processing 3 items
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 1, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token and timestamp
                Object[] state = workers.getOrDefault("Thread 1", new Object[] { resumeToken, System.nanoTime() });
                state[0] = (int)state[0] + 1;
                workers.put("Thread 1", state);
            }
        });

        // new thread spawned when the single thread pool becomes free
        executorService.execute(() -> {
            // find the least resume token
            final long currentTime = System.nanoTime();
            long maxElapsed = 0;
            String threadId = "";
            int resumeToken = 0;
            assertEquals(1, workers.size());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (elapsed > maxElapsed) {
                    resumeToken = (int) state[0];
                    threadId = entry.getKey();
                }
            }
            assertEquals(3, resumeToken);
            assertEquals("Thread 1", threadId);
            System.out.println("Thread 2 will resume " + threadId + " at: " + resumeToken);

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 2, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token and timestamp
                Object[] state = workers.get(threadId);
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.put(threadId, state);
            }
        });

        latch.await();
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }

        assertEquals(1, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            assertEquals(input.length, (int)state[0]);
        }
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, and the resumption from
     * previous crash point by another thread
     */
    @Test
    @Timeout(5)
    public void test_004() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.execute(() -> {
            int resumeToken = 0;
            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 1, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token and timestamp
                Object[] state = workers.getOrDefault("Thread 1", new Object[] { resumeToken, System.nanoTime() });
                state[0] = (int)state[0] + 1;
                workers.put("Thread 1", state);
            }
        });

        executorService.execute(() -> {
            int resumeToken = 0;
            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (cursor == 4) break; // simulate crash after processing 4 items
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 2, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token and timestamp
                Object[] state = workers.getOrDefault("Thread 2", new Object[] { resumeToken, System.nanoTime() });
                state[0] = (int)state[0] + 1;
                workers.put("Thread 2", state);
            }
        });

        executorService.execute(() -> {
            // find the least resume token
            final long currentTime = System.nanoTime();
            long maxElapsed = 0;
            String threadId = "";
            int resumeToken = 0;
            System.out.println("available workers: " + workers.keySet());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (elapsed > 100000 && elapsed > maxElapsed) {
                    resumeToken = (int) state[0];
                    threadId = entry.getKey();
                    maxElapsed = elapsed;
                }
            }
            assertEquals(4, resumeToken);
            assertEquals("Thread 2", threadId);
            System.out.println("Thread 3 will resume " + threadId + " at: " + resumeToken);

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println("Thread 3, processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token and timestamp
                Object[] state = workers.get(threadId);
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.put(threadId, state);
            }
        });

        latch.await();
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }

        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, and the resumption from
     * previous crash point by another thread.
     *
     * With improved mechanism to lookup previous
     * orphaned worker and update thread ID in the
     * workers record.
     */
    @Test
    @Timeout(5)
    public void test_005() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        Function<Void, Object[]> lookupResumeToken = (Void) -> {
            // find the least resume token
            final long currentTime = System.nanoTime();
            long minElapsed = 0;
            long maxElapsed = 0;
            String threadId = null;
            int maxResumeToken = 0;
            int minResumeToken = 0;
            System.out.println("available workers: " + workers.keySet());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (elapsed > 1000 && elapsed > maxElapsed) {
                    maxResumeToken = (int) state[0];
                    threadId = entry.getKey();
                    maxElapsed = elapsed;
                }
                if (elapsed < minElapsed) {
                    minResumeToken = (int) state[0];
                    minElapsed = elapsed;
                }
            }
            Object[] result = new Object[3];
            result[0] = threadId == null ? minResumeToken : maxResumeToken;
            result[1] = threadId;
            result[2] = threadId == null ? minElapsed : maxElapsed;
            return result;
        };

        executorService.execute(() -> {
            final String currentThreadId = "Thread 1";
            Object[] result = lookupResumeToken.apply(null);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn");
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread 2";
            Object[] result = lookupResumeToken.apply(null);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn");
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (cursor == 4) break; // simulate crash after processing 4 items
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread 3";
            Object[] result = lookupResumeToken.apply(null);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(4, resumeToken);
            assertNotNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn");
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }

        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, where a crash happened
     * during the message processing and therefore
     * the message state should be rolled back.
     *
     * The new thread which pick-up the process
     * should then resume from the rolled back
     * message to guarantee the "at-least-once"
     * semantics is held.
     *
     * Downstream message consumer should then
     * expect to receive duplicated messages.
     */
    @Test
    @Timeout(5)
    public void test_006() throws InterruptedException {
        final int[] input = new int [] {1,2,3,4,5,6,7,8,9,10};
        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(input.length);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        Function<String, Object[]> lookupResumeToken = (currentThreadId) -> {
            // find the least resume token
            final long currentTime = System.nanoTime();
            long minElapsed = 0;
            long maxElapsed = 0;
            String threadId = null;
            int maxResumeToken = 0;
            int minResumeToken = 0;
            System.out.println(currentThreadId + ", available workers: " + workers.keySet());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (elapsed > 1000 && elapsed > maxElapsed) {
                    maxResumeToken = (int) state[0];
                    threadId = entry.getKey();
                    maxElapsed = elapsed;
                }
                if (elapsed < minElapsed) {
                    minResumeToken = (int) state[0];
                    minElapsed = elapsed;
                }
            }
            Object[] result = new Object[3];
            result[0] = threadId == null ? minResumeToken : maxResumeToken;
            result[1] = threadId;
            result[2] = threadId == null ? minElapsed : maxElapsed;
            return result;
        };

        executorService.execute(() -> {
            final String currentThreadId = "Thread 1";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread 2";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    if (cursor >= 4) {
                        // simulate crash and rollback
                        processed.remove(input[cursor]);
                        break;
                    }
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread 3";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertNotNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);
                    // commit
                    processed.put(input[cursor], "COMPLETED");
                    latch.countDown();
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        });

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(10, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }

        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate scaling out with additional
     * threads with simulation of I/O latency
     * to process each message from the outbox
     */
    @Test
    @Timeout(2)
    public void test_008() throws InterruptedException {
        final int MAX_THREAD_COUNT = 3;
        final int MESSAGES_COUNT = 100;
        final int IO_WAIT_TIME_IN_MILLIS = 10;
        final int[] input = new int [MESSAGES_COUNT];
        for (int i=0; i<input.length; i++) {
            input[i] = i+1;
        }

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
//        final CountDownLatch latch = new CountDownLatch(input.length);
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        Function<String, Object[]> lookupResumeToken = (currentThreadId) -> {
            // find the least resume token
            final long currentTime = System.nanoTime();
            long minElapsed = 0;
            long maxElapsed = 0;
            String threadId = null;
            int maxResumeToken = 0;
            int minResumeToken = 0;
            System.out.println(currentThreadId + ", available workers: " + workers.keySet());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (elapsed > (long)IO_WAIT_TIME_IN_MILLIS * 1000000000L && elapsed > maxElapsed) {
                    maxResumeToken = (int) state[0];
                    threadId = entry.getKey();
                    maxElapsed = elapsed;
                }
                if (minElapsed == 0 || elapsed < minElapsed) {
                    minResumeToken = (int) state[0];
                    minElapsed = elapsed;
                }
            }
            Object[] result = new Object[3];
            result[0] = threadId == null ? minResumeToken : maxResumeToken;
            result[1] = threadId;
            result[2] = threadId == null ? minElapsed : maxElapsed;
            return result;
        };

        executorService.schedule(() -> {
            final String currentThreadId = "Thread 1";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);

                    // simulate lengthy I/O
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(IO_WAIT_TIME_IN_MILLIS);
                            } catch (InterruptedException ignored) {}
                            return null;
                        }).get();
                    } catch (InterruptedException|ExecutionException ignored) {}

                    // commit
                    processed.put(input[cursor], "COMPLETED");
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
            latch.countDown();
        }, 0, TimeUnit.MICROSECONDS);

        executorService.schedule(() -> {
            final String currentThreadId = "Thread 2";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);

                    // simulate lengthy I/O
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException ignored) {}
                            return null;
                        }).get();
                    } catch (InterruptedException|ExecutionException ignored) {}

                    // commit
                    processed.put(input[cursor], "COMPLETED");
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
            latch.countDown();
        }, 0, TimeUnit.MICROSECONDS);

        // simulate scale out of the 3rd thread after 500 ms
        executorService.schedule(() -> {
            final String currentThreadId = "Thread 3";
            Object[] result = lookupResumeToken.apply(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertTrue(resumeToken>0 && resumeToken<input.length);
            assertNull(threadId);

            if (threadId == null) {
                threadId = currentThreadId;
                System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    System.out.println(currentThreadId + ", processing: " + input[cursor]);

                    // simulate lengthy I/O
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException ignored) {}
                            return null;
                        }).get();
                    } catch (InterruptedException|ExecutionException ignored) {}

                    // commit
                    processed.put(input[cursor], "COMPLETED");
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                assertNotNull(state);
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
            latch.countDown();
        }, MESSAGES_COUNT*IO_WAIT_TIME_IN_MILLIS/2, TimeUnit.MILLISECONDS);

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(input[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }
}
