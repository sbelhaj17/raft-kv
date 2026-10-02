package raftkv.kv;

/** A client operation. Writes go through the log; Get and Status do not. */
public sealed interface Op {
    String key();

    default boolean isWrite() {
        return this instanceof Put || this instanceof Delete || this instanceof Cas;
    }

    record Get(String key) implements Op {}

    record Put(String key, String value) implements Op {}

    /** Answers ok=true if the key existed. */
    record Delete(String key) implements Op {}

    /** Set {@code key} to {@code value} if it currently holds {@code expected} (null: absent). */
    record Cas(String key, String expected, String value) implements Op {}

    /** Ask a node about itself. Answered locally, not linearizable. */
    record Status() implements Op {
        public String key() {
            return "";
        }
    }
}
