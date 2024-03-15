package cloud.xline.jxline.exceptions;

import com.xline.protobuf.ExecuteError;

import javax.annotation.Nullable;

/**
 * {@link XlineException} is a runtime exception that is thrown when there is an error when send
 * requests to xline servers
 */
public class XlineException extends RuntimeException {

    /**
     * {@link ExecuteError} when send invalid request to xline servers, maybe null when there is
     * some unknown exception happens in the runtime
     */
    private ExecuteError error;

    /**
     * Create a new {@link XlineException} with the given message
     *
     * @param cause the cause
     */
    private XlineException(Throwable cause) {
        super(cause);
    }

    /**
     * Create a new {@link XlineException} with the given {@link ExecuteError}
     *
     * @param error the {@link ExecuteError}
     */
    public XlineException(ExecuteError error) {
        this.error = error;
    }

    /**
     * Check if there is an error when send invalid request to xline servers
     *
     * @return the result
     */
    public boolean hasError() {
        return error != null;
    }

    /**
     * Get the {@link ExecuteError} when send invalid request to xline servers, maybe null when
     * there is a known exception happens in the runtime.
     *
     * @return the {@link ExecuteError}
     */
    @Nullable
    public ExecuteError getError() {
        return error;
    }

    /**
     * Map throwable to {@link XlineException}
     *
     * @param throwable the {@link io.grpc.StatusException} or some internal exceptions
     * @return the {@link XlineException}
     */
    public static XlineException toXlineException(Throwable throwable) {
        if (throwable instanceof XlineException) {
            return (XlineException) throwable;
        }
        return new XlineException(throwable);
    }
}
