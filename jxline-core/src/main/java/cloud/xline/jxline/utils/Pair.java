package cloud.xline.jxline.utils;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * A simple pair of values.
 *
 * @param <A> The first value type.
 * @param <B> The second value type.
 */
public final class Pair<A, B> {
    final A a;
    final B b;

    /**
     * Creates a new pair of values.
     *
     * @param a The first value.
     * @param b The second value.
     */
    public Pair(A a, B b) {
        this.a = Objects.requireNonNull(a);
        this.b = Objects.requireNonNull(b);
    }

    /**
     * Gets the first value.
     *
     * @return A
     */
    public A getFirst() {
        return a;
    }

    /**
     * Gets the second value.
     *
     * @return B
     */
    public B getSecond() {
        return b;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Pair<?, ?>) {
            Pair<?, ?> e = (Pair<?, ?>) o;
            return a.equals(e.getFirst()) && b.equals(e.getSecond());
        }
        return false;
    }

    /**
     * Applies a function to the pair.
     *
     * @param fn The function to apply.
     * @return The result of the function.
     * @param <T> The result type.
     */
    public <T> T apply(BiFunction<A, B, T> fn) {
        return fn.apply(a, b);
    }

    @Override
    public int hashCode() {
        return a.hashCode() ^ b.hashCode();
    }

    @Override
    public String toString() {
        return "(" + a + "," + b + ")";
    }
}
