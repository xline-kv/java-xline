package cloud.xline.jxline.utils;

import javax.annotation.Nonnull;

/**
 * Like {@link java.util.function.Function}, but throws Exceptions
 *
 * @param <R>
 * @param <V>
 */
@FunctionalInterface
public interface Invoke<R, V> {
    V call(@Nonnull R r) throws Exception;
}
