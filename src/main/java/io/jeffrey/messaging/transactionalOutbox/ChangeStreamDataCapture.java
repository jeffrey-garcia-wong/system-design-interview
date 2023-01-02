package io.jeffrey.messaging.transactionalOutbox;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ChangeStreamDataCapture {

    protected static class ChangeStreamTask implements Runnable {
        private final String currentThreadId;
        private final int[] input;
        private final ConcurrentMap<Integer, String> processed;
        private final ConcurrentMap<String, Object[]> workers;
        private int messageWaitTimeInMillis;
        private boolean debug;

        protected static class Builder {
            private String currentThreadId;
            private int[] input;
            private ConcurrentMap<Integer, String> processed;
            private ConcurrentMap<String, Object[]> workers;
            private int messageWaitTimeInMillis;
            private boolean debug = false;

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
                this.messageWaitTimeInMillis = waitTimeInMillis;
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

        private final Object[] initResult;

        private ChangeStreamTask(Builder builder) {
            this.currentThreadId = builder.currentThreadId;
            this.input = builder.input;
            this.processed = builder.processed;
            this.workers = builder.workers;
            this.messageWaitTimeInMillis = builder.messageWaitTimeInMillis;
            this.debug = builder.debug;

            // find the least resume token
            Function<String, Object[]> lookupResumeToken = (currentThreadId) -> {
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
                    if (elapsed > (long)messageWaitTimeInMillis * 1000000000L && elapsed > maxElapsed) {
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
            this.initResult = lookupResumeToken.apply(currentThreadId);
        }

        protected Object[] getInitResult() {
            return new Object[] { initResult[0], initResult[1] };
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
                Object[] state = workers.getOrDefault(threadId, new Object[]{ resumeToken, System.nanoTime() });
                state[0] = (int)state[0] + 1;
                state[1] = System.nanoTime();
                workers.remove(threadId);
                threadId = currentThreadId;
                workers.put(threadId, state);
            }
        }
    }

}
