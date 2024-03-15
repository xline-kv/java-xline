package cloud.xline.jxline.utils;

import javax.annotation.Nonnull;

/**
 * Like {@link java.util.function.Function}, but throws Exceptions
 *
 * @param <R> Input
 * @param <V> Output
 */
@FunctionalInterface
public interface Invoke<R, V> {
    /**
     * Call the function
     *
     * @param r Input
     * @return Output
     * @throws Exception If something goes wrong
     */
    V call(@Nonnull R r) throws Exception;
}
