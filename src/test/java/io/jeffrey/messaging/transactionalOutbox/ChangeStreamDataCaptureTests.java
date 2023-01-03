package io.jeffrey.messaging.transactionalOutbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
    @Timeout(1) // the product of MESSAGES_COUNT and IO_WAIT_TIME_IN_MILLIS
    public void test_001() throws InterruptedException {
        final int MAX_THREAD_COUNT = 2;
        final int MESSAGES_COUNT = 10;
        final long IO_WAIT_TIME_IN_NANOS = 10000000L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }

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
                                .debug()
                                .create();

                Object[] result = thread2.lookupResumeToken(currentThreadId);
                int resumeToken = (int) result[0];
                String threadId = (String) result[1];
                assertEquals(0, resumeToken);
                assertNull(threadId);

                thread2.run();
                latch.countDown();
            });
        }

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    @Test
    @Timeout(5)
    public void test_001_bulkMessages_maxCores() throws InterruptedException {
        final int cores = Runtime.getRuntime().availableProcessors() - 2;
        final int MAX_THREAD_COUNT = cores;
        final int MESSAGES_COUNT = 600000;
        final long IO_WAIT_TIME_IN_NANOS = 10L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }

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
                                .create();

                Object[] result = thread.lookupResumeToken(currentThreadId);
                int resumeToken = (int) result[0];
                String threadId = (String) result[1];
                assertEquals(0, resumeToken);
                assertNull(threadId);

                thread.run();
                latch.countDown();
            });
        }

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
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
                            .debug()
                            .crashAndRollback(CRASH_MESSAGE_ID)
                            .create();

            Object[] result = thread1.lookupResumeToken(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

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
                            .debug()
                            .create();

            Object[] result = thread2.lookupResumeToken(currentThreadId);
            // the crash point is not guaranteed to happen at the
            // specified position due to threads competition, instead
            // it is expected the crash should happen when the cursor
            // is equal or larger than the specified position.
            int resumeToken = (int) result[0];
            assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            String threadId = (String) result[1];
            assertNotNull(threadId); // thread-2 should pick up thread-1

            thread2.run();
            latch.countDown();
        });

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (entry.getKey().equals("Thread-2"))
                // the crash point is not guaranteed to happen at the
                // specified position due to threads competition, instead
                // it is expected the crash should happen when the cursor
                // is equal or larger than the specified position.
                assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            else
                assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate multiple threads each with its
     * own resume token, and the resumption from
     * previous crash point by an existing thread
     */
    @Test
    public void test_003() throws InterruptedException {
        
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
    @Timeout(1)
    public void test_004() throws InterruptedException {
        final int MAX_THREAD_COUNT = 2;
        final int MESSAGES_COUNT = 10;
        final long IO_WAIT_TIME_IN_NANOS = 1000000L;
        final int CRASH_MESSAGE_ID = 4;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }

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
                            .debug()
                            .create();

            Object[] result = thread1.lookupResumeToken(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

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
                            .debug()
                            .crashAndRollback(CRASH_MESSAGE_ID)
                            .create();

            Object[] result = thread2.lookupResumeToken(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

            thread2.run();
            latch.countDown();
        });

        executorService.execute(() -> {
            final String currentThreadId = "Thread-3";

            ChangeStreamDataCapture.ChangeStreamTask thread3 =
                    new ChangeStreamDataCapture.ChangeStreamTask.Builder()
                            .threadId(currentThreadId)
                            .outbox(MESSAGES)
                            .eventStore(processed)
                            .messageWaitTime(IO_WAIT_TIME_IN_NANOS)
                            .workersState(workers)
                            .debug()
                            .create();

            Object[] result = thread3.lookupResumeToken(currentThreadId);
            // the crash point is not guaranteed to happen at the
            // specified position due to threads competition, instead
            // it is expected the crash should happen when the cursor
            // is equal or larger than the specified position.
            int resumeToken = (int) result[0];
            assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            String threadId = (String) result[1];
            assertNotNull(threadId); // thread-3 should pick up thread-2

            thread3.run();
            latch.countDown();
        });

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            if (entry.getKey().equals("Thread-2"))
                // the crash point is not guaranteed to happen at the
                // specified position due to threads competition, instead
                // it is expected the crash should happen when the cursor
                // is equal or larger than the specified position.
                assertTrue(resumeToken >= CRASH_MESSAGE_ID);
            else
                assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate scaling out scenario with
     * simulation of I/O latency while processing
     * each message from the outbox
     */
    @Test
    @Timeout(1)
    public void test_005() throws InterruptedException {
        final int MAX_THREAD_COUNT = 3;
        final int MESSAGES_COUNT = 100;
        final long IO_WAIT_TIME_IN_NANOS = 1000000L;
        final int[] MESSAGES = new int [MESSAGES_COUNT];
        for (int i=0; i<MESSAGES.length; i++) {
            MESSAGES[i] = i+1;
        }

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
                            .debug()
                            .create();

            Object[] result = thread1.lookupResumeToken(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

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
                            .debug()
                            .create();

            Object[] result = thread2.lookupResumeToken(currentThreadId);
            int resumeToken = (int) result[0];
            String threadId = (String) result[1];
            assertEquals(0, resumeToken);
            assertNull(threadId);

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
                            .debug()
                            .create();

            Object[] result = thread3.lookupResumeToken(currentThreadId);
            // since this thread will be spawned at the time when the
            // exact number of processed message cannot be predicted,
            // skip validation of resume token
            String threadId = (String) result[1];
            assertNull(threadId);

            thread3.run();
            latch.countDown();
        }, MESSAGES_COUNT*IO_WAIT_TIME_IN_NANOS/5, TimeUnit.NANOSECONDS);

        latch.await();
        executorService.shutdown();
        boolean isTerminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        assertTrue(isTerminated);

        assertEquals(MESSAGES_COUNT, processed.size());
        for (int i=0; i<processed.size(); i++) {
            assertEquals("COMPLETED", processed.get(MESSAGES[i]));
        }

        assertEquals(MAX_THREAD_COUNT, workers.size());
        for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
            Object[] state = entry.getValue();
            int resumeToken = (int)state[0];
            assertEquals(MESSAGES_COUNT, resumeToken);
            System.out.println("worker: " + entry.getKey() + ", resume token: " + (int)state[0]);
        }
    }

    /**
     * Demonstrate scaling in scenario where existing thread
     * should be able to resume left-over message from the
     * terminated service
     */
    @Test
    public void test_006() throws InterruptedException {

    }

}
