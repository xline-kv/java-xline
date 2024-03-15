package cloud.xline.jxline.support;

import com.google.protobuf.ByteString;
import com.xline.protobuf.*;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.*;
import io.etcd.jetcd.support.Util;

import java.util.Optional;
import java.util.function.Consumer;

/** Requests helper */
public final class Requests {
    /**
     * Maps the PutRequest to the Command
     *
     * @param key the key
     * @param value the value
     * @param option the put option
     * @param namespace the namespace binding to the command
     * @return the command
     */
    public static Command mapPutRequest(
            ByteSequence key, ByteSequence value, PutOption option, ByteSequence namespace) {
        PutRequest req =
                PutRequest.newBuilder()
                        .setKey(Util.prefixNamespace(key, namespace))
                        .setValue(ByteString.copyFrom(value.getBytes()))
                        .setLease(option.getLeaseId())
                        .setPrevKv(option.getPrevKV())
                        .build();
        return Command.newBuilder()
                .addKeys(KeyRange.newBuilder().setKey(ByteString.copyFrom(key.getBytes())).build())
                .setRequest(
                        RequestWithToken.newBuilder().setPutRequest(req).build()) // TODO: add token
                .build();
    }

    /**
     * Maps the RangeRequest to the Command
     *
     * @param key the key
     * @param option the get option
     * @param namespace the namespace binding to the command
     * @return the command
     */
    public static Command mapRangeRequest(
            ByteSequence key, GetOption option, ByteSequence namespace) {
        RangeRequest.Builder builder =
                RangeRequest.newBuilder()
                        .setKey(Util.prefixNamespace(key, namespace))
                        .setCountOnly(option.isCountOnly())
                        .setLimit(option.getLimit())
                        .setRevision(option.getRevision())
                        .setKeysOnly(option.isKeysOnly())
                        .setSerializable(option.isSerializable())
                        .setSortOrder(toRangeRequestSortOrder(option.getSortOrder()))
                        .setSortTarget(toRangeRequestSortTarget(option.getSortField()))
                        .setMinCreateRevision(option.getMinCreateRevision())
                        .setMaxCreateRevision(option.getMaxCreateRevision())
                        .setMinModRevision(option.getMinModRevision())
                        .setMaxModRevision(option.getMaxModRevision());

        defineRangeRequestEnd(
                key, option.getEndKey(), option.isPrefix(), namespace, builder::setRangeEnd);
        return Command.newBuilder()
                .addKeys(KeyRange.newBuilder().setKey(ByteString.copyFrom(key.getBytes())).build())
                .setRequest(
                        RequestWithToken.newBuilder()
                                .setRangeRequest(builder.build())
                                .build()) // TODO: add token
                .build();
    }

    /**
     * Maps the DeleteRequest to the Command
     *
     * @param key the key
     * @param option the delete option
     * @param namespace the namespace binding to the command
     * @return the command
     */
    public static Command mapDeleteRequest(
            ByteSequence key, DeleteOption option, ByteSequence namespace) {
        DeleteRangeRequest.Builder builder =
                DeleteRangeRequest.newBuilder()
                        .setKey(Util.prefixNamespace(key, namespace))
                        .setPrevKv(option.isPrevKV());

        defineRangeRequestEnd(
                key, option.getEndKey(), option.isPrefix(), namespace, builder::setRangeEnd);

        return Command.newBuilder()
                .addKeys(KeyRange.newBuilder().setKey(ByteString.copyFrom(key.getBytes())).build())
                .setRequest(
                        RequestWithToken.newBuilder()
                                .setDeleteRangeRequest(builder.build())
                                .build()) // TODO: add token
                .build();
    }

    /**
     * Maps the TxnRequest to the Command
     *
     * @param revision the revision
     * @param option the txn option
     * @return the command
     */
    public static Command mapCompactRequest(long revision, CompactOption option) {
        CompactionRequest req =
                CompactionRequest.newBuilder()
                        .setRevision(revision)
                        .setPhysical(option.isPhysical())
                        .build();
        return Command.newBuilder()
                .setRequest(
                        RequestWithToken.newBuilder()
                                .setCompactionRequest(req)
                                .build()) // TODO: add token
                .build();
    }

    private static RangeRequest.SortOrder toRangeRequestSortOrder(GetOption.SortOrder order) {
        switch (order) {
            case NONE:
                return RangeRequest.SortOrder.NONE;
            case ASCEND:
                return RangeRequest.SortOrder.ASCEND;
            case DESCEND:
                return RangeRequest.SortOrder.DESCEND;
            default:
                return RangeRequest.SortOrder.UNRECOGNIZED;
        }
    }

    public static RangeRequest.SortTarget toRangeRequestSortTarget(GetOption.SortTarget target) {
        switch (target) {
            case KEY:
                return RangeRequest.SortTarget.KEY;
            case CREATE:
                return RangeRequest.SortTarget.CREATE;
            case MOD:
                return RangeRequest.SortTarget.MOD;
            case VALUE:
                return RangeRequest.SortTarget.VALUE;
            case VERSION:
                return RangeRequest.SortTarget.VERSION;
            default:
                return RangeRequest.SortTarget.UNRECOGNIZED;
        }
    }

    private static void defineRangeRequestEnd(
            ByteSequence key,
            Optional<ByteSequence> endKeyOptional,
            boolean hasPrefix,
            ByteSequence namespace,
            Consumer<ByteString> setRangeEndConsumer) {

        if (endKeyOptional.isPresent()) {
            setRangeEndConsumer.accept(
                    Util.prefixNamespaceToRangeEnd(
                            ByteString.copyFrom(endKeyOptional.get().getBytes()), namespace));
        } else {
            if (hasPrefix) {
                ByteSequence endKey = OptionsUtil.prefixEndOf(key);
                setRangeEndConsumer.accept(
                        Util.prefixNamespaceToRangeEnd(
                                ByteString.copyFrom(endKey.getBytes()), namespace));
            }
        }
    }
}
