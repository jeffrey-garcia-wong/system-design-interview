package io.jeffrey.messaging.transactionalOutbox;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

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
 * where M is the total number of messages, while each node needs
 * to process M messages and the read/write of the hash table is
 * an O(1) operation, a total time complexity of O(M) is expected.<p/>
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
 * helps to decide where the newly spawned node (scale-out scenario)
 * should start picking up message such that to avoid re-processing
 * the entire outbox. To search the resumption point, a scanning
 * of all the values in the workers hashtable is required, the additional
 * lookup incurs a constant search time of O(1), assume there is
 * only single node, this searching is a constant time operation.
 * <p/>
 *
 * <h3>Overall time complexity</h3>
 * <pre>
 * {@code
 * Overall time complexity is O(M) *O(N) since this is the minimal
 * unit of work each node must do, when N = 1 (single node), the time
 * complexity is O(M) which depends on the number of messages.
 * If we think of number of nodes is fixed in a cloud environment,
 * the linear time complexity can always be held.
 * }
 * </pre>
 *
 * <h3>Overall space complexity</h3>
 * <pre>
 * {@code
 * O(M + N) because there are 2 hash tables of size M and N, this
 * remains the same no matter how many running nodes.
 * }
 * </pre>
 */
public class ChangeStreamDataCapture {

    protected static class ChangeStreamTask implements Callable<Void> {
        private final String currentThreadId;
        private final int[] input;
        private final ConcurrentMap<Integer, String> processed;
        private final ConcurrentMap<String, Object[]> workers;
        private final long messageWaitTimeInNanos;
        private final boolean debug;
        private final int crashAndRollback;

        protected static class Builder {
            private String currentThreadId;
            private int[] input;
            private ConcurrentMap<Integer, String> processed;
            private ConcurrentMap<String, Object[]> workers;
            private long messageWaitTimeInNanos;
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
            public Builder messageWaitTime(long waitTimeInNanos) {
                // the wait time simulate the I/O latency,
                // should not be less than zero
                if (waitTimeInNanos <= 0) {
                    System.err.println("wait time should be larger than zero, default to 1 millisecond");
                    waitTimeInNanos = 1000000L;
                }
                this.messageWaitTimeInNanos = waitTimeInNanos;
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
            public Builder debug(boolean debug) {
                this.debug = debug;
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
            this.messageWaitTimeInNanos = builder.messageWaitTimeInNanos;
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
                try {
                    Object[] state = entry.getValue();
                    int lastResumeToken = (int) state[0];
                    long lastElapsedTime = currentTime - (long)state[1];
                    if (lastResumeToken >= input.length) {
                        // worker has processed all the input
                        minResumeToken = (int) state[0];
                        minElapsed = lastElapsedTime;
                        break;
                    }
                    if (maxElapsed == 0 || lastElapsedTime > maxElapsed) {
                        // only resume token that was completed is persisted in the event store
                        if (!processed.containsKey(input[(int) state[0]])) {
                            maxResumeToken = (int) state[0];
                            resumeThreadId = entry.getKey();
                            maxElapsed = lastElapsedTime;
                        }
                    }
                    if (minElapsed == 0 || lastElapsedTime < minElapsed) {
                        minResumeToken = (int) state[0];
                        minElapsed = lastElapsedTime;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
        public Void call() throws Exception {
            Object[] initResult = lookupResumeToken(this.currentThreadId);
            int resumeToken = (int) initResult[0];
            String threadId = (String) initResult[1];

            if (threadId == null) {
                threadId = currentThreadId;
                if (debug) System.out.println(threadId + " will spawn at: " + resumeToken);
            } else {
                if (debug) System.out.println(currentThreadId + " will resume " + threadId + " at: " + resumeToken);
            }

            for (int cursor=resumeToken; cursor<input.length; cursor++) {
                // de-duplication
                if (!processed.containsKey(input[cursor])) {
                    processed.put(input[cursor], "PROCESSING");
                    if (debug) System.out.println(currentThreadId + ", processing: " + input[cursor]);

                    if (crashAndRollback>=0 && crashAndRollback <= cursor) {
                        // simulate crash and rollback
                        processed.remove(input[cursor]);
                        break;
                    }

                    // simulate lengthy I/O
//                    try {
                        CompletableFuture.supplyAsync(() -> {
                            // use busy wait to provide fine-grained
                            // control of waiting time to milliseconds
                            long startTime = System.nanoTime();
                            while (System.nanoTime() - startTime <= messageWaitTimeInNanos) {
                            }
                            return null;
                        }).get();
//                    } catch (InterruptedException ignored) {}

                    // commit
                    processed.put(input[cursor], "COMPLETED");
                }

                // ensure immutability
                Object[] newState;
                if (workers.containsKey(threadId)) {
                    Object[] state = workers.get(threadId);
                    newState = new Object[] {(int) state[0], (long)state[1] };
                } else {
                    newState = new Object[] { resumeToken, System.nanoTime() };
                }
                // update resume token timestamp and threadId
                newState[0] = cursor + 1;
                newState[1] = System.nanoTime();
                if (!threadId.equals(currentThreadId)) {
                    threadId = currentThreadId;
                }
                workers.put(threadId, newState);
            }

            return null;
        }
    }

}
