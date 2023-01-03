package io.jeffrey.messaging.transactionalOutbox;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * <h2>Algorithms Design:</h2><hr/>
 * Since the transactional outbox implementation will be embedded
 * into service which runs an arbitrary number of nodes in cloud,
 * the solution must fulfill the following requirements:
 * <ul>
 *     <li>
 *         able to resume processing of message from previous crash
 *     </li>
 *     <li>
 *         avoid duplicate processing of messages that has already
 *         been processed
 *     </li>
 *     <li>
 *         in a scale-out scenario, newly spawned instances should
 *         process messages after the recently processed message,
 *         avoid re-processing the entire outbox
 *     </li>
 *     <li>
 *         in a scale-in scenario, remaining instances should be
 *         able to pick up messages from where the terminated service
 *         left off.
 *     </li>
 * </ul>
 *
 * <h2>Performance:</h2><hr/>
 * To avoid duplicate processing, we need to keep track of all
 * processed message, a hash table which record the state of each
 * message is required, the space complexity of this would be O(M)
 * where M is the total number of messages, while read/write of the
 * hash table incurs a time complexity of O(1).<p/>
 *
 * To be able to resume processing of message from previous crash,
 * we need to keep track of the last processed message of each
 * running nodes (workers), a hash table is required for every
 * node, which records the latest processed message identifier
 * and its execution timestamp.<p/>
 *
 * Timestamp is essential since it's an important clue indicating
 * that a node is possibly down by computing the difference of
 * every timestamp with current timestamp (elapsed time). We first
 * examine the maximum value, which is the worker with oldest
 * execution timestamp, next we perform an additional lookup into
 * the hash table of processed messages using the corresponding
 * message identifier.<p/>
 *
 * If the message cannot be found, the message was left off from
 * another node which was terminated, and must be pick up by the
 * newly spawned node.
 * <pre>
 * {@code
 * This additional lookup guard against the scenario when there
 * is no new message to be processed and new node is spawned to
 * replace old node terminated by normal cluster maintenance
 * activity.
 * }
 * </pre>
 *
 * Next we examine the minimum value of elapsed time, this information
 * helps to decide where the newly spawned service (scale-out scenario)
 * should start picking up message such that to avoid re-processing
 * the entire outbox. To search the resumption point, a scanning
 * of all the values in the workers hashtable is required, the additional
 * lookup incurs a constant search time of O(1), therefore the time
 * complexity is O(N) where N is the number of nodes, space complexity
 * is O(N).
 * <p/>
 *
 * <h3>Overall time complexity</h3>
 * <pre>
 * {@code
 * O(N * N) because there is N number of nodes which performs
 * the search on N nodes
 * }
 * </pre>
 *
 * <h3>Overall space complexity</h3>
 * <pre>
 * {@code
 * O(M + N) because there are 2 hash tables of size M and N
 * }
 * </pre>
 */
public class ChangeStreamDataCapture {

    protected static class ChangeStreamTask implements Runnable {
        private final String currentThreadId;
        private final int[] input;
        private final ConcurrentMap<Integer, String> processed;
        private final ConcurrentMap<String, Object[]> workers;
        private final int messageWaitTimeInMillis;
        private final boolean debug;
        private final int crashAndRollback;

        protected static class Builder {
            private String currentThreadId;
            private int[] input;
            private ConcurrentMap<Integer, String> processed;
            private ConcurrentMap<String, Object[]> workers;
            private int messageWaitTimeInMillis;
            private boolean debug = false;
            private int crashAndRollback = -1;

            protected Builder() {}
            public Builder threadId(String threadId) {
                this.currentThreadId = threadId;
                return this;
            }
            public Builder outbox(int[] input) {
                this.input = input;
                return this;
            }
            public Builder eventStore(ConcurrentMap<Integer, String> processed) {
                this.processed = processed;
                return this;
            }
            public Builder workersState(ConcurrentMap<String, Object[]> workers) {
                this.workers = workers;
                return this;
            }
            public Builder messageWaitTime(int waitTimeInMillis) {
                // the wait time simulate the I/O latency,
                // should not be less than zero
                if (waitTimeInMillis <= 0) {
                    System.err.println("wait time must be at least 1 millisecond, resetting to 1");
                    waitTimeInMillis = 1;
                }
                this.messageWaitTimeInMillis = waitTimeInMillis;
                return this;
            }
            public Builder crashAndRollback(int crashAndRollback) {
                // the crash point is not guaranteed to happen at the
                // specified position due to threads competition, instead
                // it is expected the crash should happen when the cursor
                // is equal or larger than the specified position.
                this.crashAndRollback = crashAndRollback;
                return this;
            }
            public Builder debug() {
                this.debug = true;
                return this;
            }
            public ChangeStreamTask create() {
                return new ChangeStreamTask(this);
            }
        }

        private ChangeStreamTask(Builder builder) {
            this.currentThreadId = builder.currentThreadId;
            this.input = builder.input;
            this.processed = builder.processed;
            this.workers = builder.workers;
            this.messageWaitTimeInMillis = builder.messageWaitTimeInMillis;
            this.debug = builder.debug;
            this.crashAndRollback = builder.crashAndRollback;
        }

        private Object[] initResult;

        // find the least resume token
        protected Object[] lookupResumeToken(String currentThreadId) {
            final long currentTime = System.nanoTime();
            long minElapsed = 0;
            long maxElapsed = 0;
            String resumeThreadId = null;
            int maxResumeToken = 0;
            int minResumeToken = 0;
            if (debug) System.out.println(currentThreadId + ", available workers: " + workers.keySet());
            for (Map.Entry<String, Object[]> entry : workers.entrySet()) {
                Object[] state = entry.getValue();
                long elapsed = currentTime - (long)state[1];
                if (maxElapsed == 0 || elapsed > maxElapsed) {
                    // only resume token that was completed is persisted in the event store
                    if (!processed.containsKey(input[(int) state[0]])) {
                        maxResumeToken = (int) state[0];
                        resumeThreadId = entry.getKey();
                        maxElapsed = elapsed;
                    }
                }
                if (minElapsed == 0 || elapsed < minElapsed) {
                    minResumeToken = (int) state[0];
                    minElapsed = elapsed;
                }
            }
            Object[] result = new Object[3];
            result[0] = resumeThreadId == null ? minResumeToken : maxResumeToken;
            result[1] = resumeThreadId;
            result[2] = resumeThreadId == null ? minElapsed : maxElapsed;

            this.initResult = result;
            return new Object[] { result[0], result[1], result[2] };
        }

        @Override
        public void run() {
            int resumeToken = (int) initResult[0];
            String threadId = (String) initResult[1];

            if (threadId == null) {
                threadId = currentThreadId;
                if (debug) System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                if (debug) System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    if (debug) System.out.println(currentThreadId + ", processing: " + input[cursor]);

                    if (crashAndRollback>=0 && crashAndRollback <= cursor) {
                        // simulate crash and rollback
                        processed.remove(input[cursor]);
                        break;
                    }

                    // simulate lengthy I/O
                    try {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(messageWaitTimeInMillis);
                            } catch (InterruptedException ignored) {}
                            return null;
                        }).get();
                    } catch (InterruptedException | ExecutionException ignored) {}

                    // commit
                    processed.put(input[cursor], "COMPLETED");
                }
                // update resume token timestamp and threadId
                Object[] state = workers.getOrDefault(threadId, new Object[] { resumeToken, System.nanoTime() });
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        }
    }

}
