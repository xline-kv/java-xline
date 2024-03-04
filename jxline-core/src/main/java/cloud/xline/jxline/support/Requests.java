package cloud.xline.jxline.support;

import com.google.protobuf.ByteString;
import com.xline.protobuf.*;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.*;
import io.etcd.jetcd.support.Util;

import java.util.Optional;
import java.util.function.Consumer;

public final class Requests {

    public static PutRequest mapPutRequest(
            ByteSequence key, ByteSequence value, PutOption option, ByteSequence namespace) {
        return PutRequest.newBuilder()
                .setKey(Util.prefixNamespace(key, namespace))
                .setValue(ByteString.copyFrom(value.getBytes()))
                .setLease(option.getLeaseId())
                .setPrevKv(option.getPrevKV())
                .build();
    }

    public static Command mapPutCommand(
            ByteSequence key, ByteSequence value, PutOption option, ByteSequence namespace) {
        PutRequest req = mapPutRequest(key, value, option, namespace);
        return Command.newBuilder()
                .addKeys(KeyRange.newBuilder().setKey(ByteString.copyFrom(key.getBytes())).build())
                .setRequest(
                        RequestWithToken.newBuilder().setPutRequest(req).build()) // TODO: add token
                .build();
    }

    public static RangeRequest.Builder mapRangeRequest(
            ByteSequence key, GetOption option, ByteSequence namespace) {
        return RangeRequest.newBuilder()
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
    }

    public static Command mapRangeCommand(
            ByteSequence key, GetOption option, ByteSequence namespace) {
        RangeRequest.Builder builder = mapRangeRequest(key, option, namespace);

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

    public static DeleteRangeRequest.Builder mapDeleteRequest(
            ByteSequence key, DeleteOption option, ByteSequence namespace) {
        return DeleteRangeRequest.newBuilder()
                .setKey(Util.prefixNamespace(key, namespace))
                .setPrevKv(option.isPrevKV());
    }

    public static Command mapDeleteCommand(
            ByteSequence key, DeleteOption option, ByteSequence namespace) {
        DeleteRangeRequest.Builder builder = mapDeleteRequest(key, option, namespace);
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
