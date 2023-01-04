package io.jeffrey.messaging.transactionalOutbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeStreamDataCaptureTests {

    @BeforeEach
    public void reset() {}

    /**
     * Base test case which demonstrate the normal flow (happy path)
     * where the messages are distributed among the threads and fully
     * processed.
     */
    @Test
    @Execution(ExecutionMode.CONCURRENT)
    @Timeout(1)
    public void test_001() throws InterruptedException {
        final int MAX_THREAD_COUNT = 2;
        final int MESSAGES_COUNT = 10;
        final long IO_WAIT_TIME_IN_NANOS = 10000000L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        for (int i=0; i<MAX_THREAD_COUNT; i++) {
            final int id = i;
            executorService.execute(() -> {
                final String currentThreadId = "Thread-" + (id);
                ChangeStreamDataCapture.ChangeStreamTask thread2 =
                        new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                                .threadId(currentThreadId)
                                .outbox(MESSAGES)
                                .eventStore(processed)
                                .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                                .workersState(workers)
                                .debug(DEBUG)
                                .create();

                @SuppressWarnings("unused")
                Object[] result = thread2.lookupResumeToken(currentThreadId);

                thread2.run();
                latch.countDown();
            });
        }

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
            assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(1000)
    public void test_001_repeat() throws InterruptedException {
        test_001();
    }

    @Test
    @Execution(ExecutionMode.CONCURRENT)
    @Timeout(10)
    public void test_001_bulkMessages_maxCores() throws InterruptedException {
        final int CPU_CORES = Runtime.getRuntime().availableProcessors() - 2;
        final int MAX_THREAD_COUNT = CPU_CORES;
        final int MESSAGES_COUNT = 100000;
        final long IO_WAIT_TIME_IN_NANOS = 10L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        for (int i=0; i<MAX_THREAD_COUNT; i++) {
            final int id = i;
            executorService.execute(() -> {
                final String currentThreadId = "Thread-" + (id);
                ChangeStreamDataCapture.ChangeStreamTask thread =
                        new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                                .threadId(currentThreadId)
                                .outbox(MESSAGES)
                                .eventStore(processed)
                                .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                                .workersState(workers)
                                .debug(DEBUG)
                                .create();

                @SuppressWarnings("unused")
                Object[] result = thread.lookupResumeToken(currentThreadId);

                thread.run();
                latch.countDown();
            });
        }

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
            assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    /**
     * Demonstrate single thread crash and
     * resumption from previous crash point
     * by newly spawned thread
     */
    @Test
    @Timeout(1)
    public void test_002() throws InterruptedException {
        final int MAX_THREAD_COUNT = 1;
        final int MESSAGES_COUNT = 10;
        final int IO_WAIT_TIME_IN_MILLIS = 10;
        final int CRASH_MESSAGE_ID = 4;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.execute(() -> {
            final String currentThreadId = "Thread-1";
            ChangeStreamDataCapture.ChangeStreamTask thread1 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_MILLIS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .crashAndRollback(CRASH_MESSAGE_ID)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread1.lookupResumeToken(currentThreadId);

            thread1.run();
            latch.countDown();
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread-2";
            ChangeStreamDataCapture.ChangeStreamTask thread2 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_MILLIS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            // the crash point is not guaranteed to happen at the
            // specified position due to threads competition, instead
            // it is expected the crash should happen when the cursor
            // is equal or larger than the specified position.
            @SuppressWarnings("unused")
            Object[] result = thread2.lookupResumeToken(currentThreadId);

            thread2.run();
            latch.countDown();
        });

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT + 1, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
            if (entry.getKey().equals("Thread-1"))
                // the crash point is not guaranteed to happen at the
                // specified position due to threads competition, instead
                // it is expected the crash should happen when the cursor
                // is equal or larger than the specified position.
                assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            else
                assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(1000)
    public void test_002_repeat() throws InterruptedException {
        test_002();
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, and the resumption from
     * previous crash point by an existing thread
     */
    @Test
    @Execution(ExecutionMode.CONCURRENT)
    @Timeout(1)
    public void test_003() throws InterruptedException {
        final int MAX_THREAD_COUNT = 2;
        final int MESSAGES_COUNT = 10;
        final long IO_WAIT_TIME_IN_NANOS = 1000000L;
        final int CRASH_MESSAGE_ID = 4;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.execute(() -> {
            final String currentThreadId = "Thread-1";
            ChangeStreamDataCapture.ChangeStreamTask thread1 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .crashAndRollback(CRASH_MESSAGE_ID)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread1.lookupResumeToken(currentThreadId);

            thread1.run();
            latch.countDown();
        });

        executorService.schedule(() -> {
            final String currentThreadId = "Thread-2";
            ChangeStreamDataCapture.ChangeStreamTask thread2 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            /*
             * the crash point is not guaranteed to happen at the
             * specified position due to threads competition
             */
            @SuppressWarnings("unused")
            Object[] result = thread2.lookupResumeToken(currentThreadId);

            thread2.run();
            latch.countDown();
        }, MESSAGES_COUNT * IO_WAIT_TIME_IN_NANOS, TimeUnit.NANOSECONDS);

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        /*
         * By time when thread 2 spawned, there are few
         * possible outcomes:
         * - thread 2 pickup the message where thread 1 left off
         * - thread 2 spawned before thread 1 and finish process
         *   all messages, so thread 1 ended-up not processing
         *   any message
         */
        if (workers.size() == 1) {
            assertTrue(workers.containsKey("Thread-2"));
        } else if (workers.size() == 2) {
            assertTrue(workers.containsKey("Thread-1"));
            assertTrue(workers.containsKey("Thread-2"));
        } else {
            fail("invalid workers size: " + workers.size());
        }
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + resumeToken);
            if (entry.getKey().equals("Thread-1"))
                /*
                 * the crash point is not guaranteed to happen at the
                 * specified position due to threads competition
                 */
                assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            else
                assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(1000)
    public void test_003_repeat() throws InterruptedException {
        test_003();
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, where a crash happened
     * during the message processing and therefore
     * the message state should be rolled back.
     *
     * The newly spawned thread which pick-up the
     * process should then resume from the rolled
     * back message to guarantee the "at-least-once"
     * semantics is held.
     *
     * Downstream message consumer should then
     * expect to receive duplicated messages.
     */
    @Test
    @Execution(ExecutionMode.CONCURRENT)
    @Timeout(2)
    public void test_004() throws InterruptedException {
        final int MAX_THREAD_COUNT = 2;
        final int MESSAGES_COUNT = 10;
        final long IO_WAIT_TIME_IN_NANOS = 1000000L;
        final int CRASH_MESSAGE_ID = 4;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        // latch count MUST BE EQUAL to the number of workers been spawned
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT + 1);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.execute(() -> {
            final String currentThreadId = "Thread-1";
            ChangeStreamDataCapture.ChangeStreamTask thread1 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread1.lookupResumeToken(currentThreadId);

            thread1.run();
            latch.countDown();
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread-2";
            ChangeStreamDataCapture.ChangeStreamTask thread2 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .crashAndRollback(CRASH_MESSAGE_ID)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread2.lookupResumeToken(currentThreadId);

            thread2.run();
            latch.countDown();
        });

        executorService.schedule(() -> {
            final String currentThreadId = "Thread-3";
            ChangeStreamDataCapture.ChangeStreamTask thread3 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread3.lookupResumeToken(currentThreadId);

            thread3.run();
            latch.countDown();
        }, (CRASH_MESSAGE_ID) * IO_WAIT_TIME_IN_NANOS, TimeUnit.NANOSECONDS);

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        /*
         * By time when thread 3 spawned, there are few
         * possible outcomes:
         * - thread 3 pickup the message where thread 2 left off
         * - thread 1 pickup the message where thread 2 left off,
         *   and thread 3 pickup any message after the crash point
         * - thread 1 finish process all messages, so thread 3
         *   ended-up not processing any message
         */
        if (workers.size() == 2) {
            // Thread 3 pickup Thread 2
            assertTrue(workers.containsKey("Thread-1"));
            assertTrue(workers.containsKey("Thread-3") || workers.containsKey("Thread-2"));
        } else if (workers.size() == 3) {
            // Thread 1 pickup Thread 2
            assertTrue(workers.containsKey("Thread-1"));
            assertTrue(workers.containsKey("Thread-2"));
            assertTrue(workers.containsKey("Thread-3"));
        } else {
            fail("invalid workers size: " + workers.size());
        }
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + resumeToken);
            if (entry.getKey().equals("Thread-2"))
                /*
                 * the crash point is not guaranteed to happen at the
                 * specified position due to threads competition
                 */
                assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            else
                assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    @Execution(ExecutionMode.CONCURRENT)
    @RepeatedTest(1000)
    public void test_004_repeat() throws InterruptedException {
        test_004();
    }

    /**
     * Demonstrate scaling out scenario with
     * simulation of I/O latency while processing
     * each message from the outbox
     */
    @Test
    @Execution(ExecutionMode.CONCURRENT)
    @Timeout(1)
    public void test_005() throws InterruptedException {
        final int MAX_THREAD_COUNT = 3;
        final int MESSAGES_COUNT = 100;
        final long IO_WAIT_TIME_IN_NANOS = 1000L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }
        final boolean DEBUG = false;

        final ConcurrentMap<Integer, String> processed = new ConcurrentHashMap<>();
        final CountDownLatch latch = new CountDownLatch(MAX_THREAD_COUNT);
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(MAX_THREAD_COUNT);
        final ConcurrentMap<String, Object[]> workers = new ConcurrentHashMap<>();

        executorService.schedule(() -> {
            final String currentThreadId = "Thread-1";
            ChangeStreamDataCapture.ChangeStreamTask thread1 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread1.lookupResumeToken(currentThreadId);

            thread1.run();
            latch.countDown();
        }, 0, TimeUnit.MICROSECONDS);

        executorService.schedule(() -> {
            final String currentThreadId = "Thread-2";
            ChangeStreamDataCapture.ChangeStreamTask thread2 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread2.lookupResumeToken(currentThreadId);

            thread2.run();
            latch.countDown();
        }, 0, TimeUnit.MICROSECONDS);

        // simulate scale out of the 3rd thread after some time
        executorService.schedule(() -> {
            final String currentThreadId = "Thread-3";

            ChangeStreamDataCapture.ChangeStreamTask thread3 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug(DEBUG)
                            .create();

            @SuppressWarnings("unused")
            Object[] result = thread3.lookupResumeToken(currentThreadId);

            thread3.run();
            latch.countDown();
        }, MESSAGES_COUNT*IO_WAIT_TIME_IN_NANOS/5, TimeUnit.NANOSECONDS);

        latch.await();
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1);
        }
        executorService.shutdownNow();

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        /*
         * By time when thread 3 spawned, there are few
         * possible outcomes:
         * - thread 1, 2, 3 together finish all the messages
         * - thread 1 and 2 finish all the messages, so thread 3
         *   ended-up not processing any message
         * - thread 1 finish process all messages. so thread 2
         *   and 3 ended-up not processing any message
         * - thread 2 finish process all messages. so thread 1
         *   and 3 ended-up not processing any message
         */
        if (workers.size() == 1) {
            assertTrue(workers.containsKey("Thread-1") || workers.containsKey("Thread-2"));
        } else if (workers.size() == 2) {
            assertTrue(workers.containsKey("Thread-1"));
            assertTrue(workers.containsKey("Thread-2"));
        } else if (workers.size() == 3) {
            assertTrue(workers.containsKey("Thread-1"));
            assertTrue(workers.containsKey("Thread-2"));
            assertTrue(workers.containsKey("Thread-3"));
        } else {
            fail("invalid workers size: " + workers.size());
        }
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (DEBUG) System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
            assertEquals(MESSAGES_COUNT, resumeToken);
        }
    }

    @Execution(ExecutionMode.SAME_THREAD)
    @RepeatedTest(1000)
    public void test_005_repeat() throws InterruptedException {
        test_005();
    }

    /**
     * Demonstrate scaling in scenario where existing thread
     * should be able to resume left-over message from the
     * terminated service
     */
//    @Test
//    public void test_006() throws InterruptedException {
//
//    }

}
