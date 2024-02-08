package cloud.xline.jxline;

import com.xline.protobuf.Command;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.support.CloseableClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

public interface ProtocolClient extends CloseableClient {
    <T> CompletableFuture<T> propose(
            Command cmd, boolean useFastPath, BiFunction<CommandResponse, SyncResponse, T> convert);
}
