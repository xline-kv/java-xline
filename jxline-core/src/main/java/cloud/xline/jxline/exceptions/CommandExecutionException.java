package cloud.xline.jxline.exceptions;

import com.xline.protobuf.ExecuteError;

public class CommandExecutionException extends RuntimeException {
    private final ExecuteError err;

    public CommandExecutionException(ExecuteError err) {
        this.err = err;
    }

    /**
     * Get the execute error
     *
     * @return ExecuteError
     */
    public ExecuteError getErr() {
        return err;
    }
}
