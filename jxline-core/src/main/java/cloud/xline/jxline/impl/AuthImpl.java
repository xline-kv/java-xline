package cloud.xline.jxline.impl;

import cloud.xline.jxline.Auth;
import cloud.xline.jxline.ProtocolClient;
import cloud.xline.jxline.auth.AuthDisableResponse;
import cloud.xline.jxline.auth.AuthEnableResponse;
import cloud.xline.jxline.auth.AuthRoleAddResponse;
import cloud.xline.jxline.auth.AuthRoleDeleteResponse;
import cloud.xline.jxline.auth.AuthRoleGetResponse;
import cloud.xline.jxline.auth.AuthRoleGrantPermissionResponse;
import cloud.xline.jxline.auth.AuthRoleListResponse;
import cloud.xline.jxline.auth.AuthRoleRevokePermissionResponse;
import cloud.xline.jxline.auth.AuthUserAddResponse;
import cloud.xline.jxline.auth.AuthUserChangePasswordResponse;
import cloud.xline.jxline.auth.AuthUserDeleteResponse;
import cloud.xline.jxline.auth.AuthUserGetResponse;
import cloud.xline.jxline.auth.AuthUserGrantRoleResponse;
import cloud.xline.jxline.auth.AuthUserListResponse;
import cloud.xline.jxline.auth.AuthUserRevokeRoleResponse;
import cloud.xline.jxline.auth.Permission;
import com.google.protobuf.ByteString;
import com.xline.protobuf.*;
import io.etcd.jetcd.ByteSequence;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class AuthImpl extends Impl implements Auth {

    private final ProtocolClient protocolClient;

    AuthImpl(ProtocolClient protocolClient, ClientConnectionManager connectionManager) {
        super(connectionManager);
        this.protocolClient = protocolClient;
    }

    @Override
    public CompletableFuture<AuthEnableResponse> authEnable() {
        AuthEnableRequest req = AuthEnableRequest.getDefaultInstance();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthEnableRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthEnableResponse::new);
    }

    @Override
    public CompletableFuture<AuthDisableResponse> authDisable() {
        AuthDisableRequest req = AuthDisableRequest.getDefaultInstance();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthDisableRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthDisableResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserAddResponse> userAdd(
            ByteSequence user, ByteSequence password) {
        requireNonNull(user, "user can't be null");
        requireNonNull(password, "password can't be null");

        AuthUserAddRequest req =
                AuthUserAddRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(user.getBytes()))
                        .setPasswordBytes(ByteString.copyFrom(password.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserAddRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserAddResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserDeleteResponse> userDelete(ByteSequence user) {
        requireNonNull(user, "user can't be null");

        AuthUserDeleteRequest req = AuthUserDeleteRequest.newBuilder().build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserDeleteRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserDeleteResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserChangePasswordResponse> userChangePassword(
            ByteSequence user, ByteSequence password) {
        requireNonNull(user, "user can't be null");
        requireNonNull(password, "password can't be null");

        AuthUserChangePasswordRequest req =
                AuthUserChangePasswordRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(user.getBytes()))
                        .setPasswordBytes(ByteString.copyFrom(password.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(
                                RequestWithToken.newBuilder().setAuthUserChangePasswordRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserChangePasswordResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserGetResponse> userGet(ByteSequence user) {
        requireNonNull(user, "user can't be null");

        AuthUserGetRequest req =
                AuthUserGetRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(user.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserGetRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserGetResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserListResponse> userList() {
        AuthUserListRequest req = AuthUserListRequest.getDefaultInstance();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserListRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserListResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserGrantRoleResponse> userGrantRole(
            ByteSequence user, ByteSequence role) {
        requireNonNull(user, "user can't be null");
        requireNonNull(role, "role can't be null");

        AuthUserGrantRoleRequest req =
                AuthUserGrantRoleRequest.newBuilder()
                        .setUserBytes(ByteString.copyFrom(user.getBytes()))
                        .setRoleBytes(ByteString.copyFrom(role.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserGrantRoleRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserGrantRoleResponse::new);
    }

    @Override
    public CompletableFuture<AuthUserRevokeRoleResponse> userRevokeRole(
            ByteSequence user, ByteSequence role) {
        requireNonNull(user, "user can't be null");
        requireNonNull(role, "role can't be null");

        AuthUserRevokeRoleRequest req =
                AuthUserRevokeRoleRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(user.getBytes()))
                        .setRoleBytes(ByteString.copyFrom(role.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthUserRevokeRoleRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthUserRevokeRoleResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleAddResponse> roleAdd(ByteSequence role) {
        requireNonNull(role, "role can't be null");

        AuthRoleAddRequest req =
                AuthRoleAddRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(role.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthRoleAddRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleAddResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleGrantPermissionResponse> roleGrantPermission(
            ByteSequence role, ByteSequence key, ByteSequence rangeEnd, Permission.Type permType) {
        requireNonNull(role, "role can't be null");
        requireNonNull(key, "key can't be null");
        requireNonNull(rangeEnd, "rangeEnd can't be null");
        requireNonNull(permType, "permType can't be null");

        com.xline.protobuf.Permission.Type type;
        switch (permType) {
            case WRITE:
                type = com.xline.protobuf.Permission.Type.WRITE;
                break;
            case READWRITE:
                type = com.xline.protobuf.Permission.Type.READWRITE;
                break;
            case READ:
                type = com.xline.protobuf.Permission.Type.READ;
                break;
            default:
                type = com.xline.protobuf.Permission.Type.UNRECOGNIZED;
                break;
        }

        com.xline.protobuf.Permission perm =
                com.xline.protobuf.Permission.newBuilder()
                        .setKey(ByteString.copyFrom(key.getBytes()))
                        .setRangeEnd(ByteString.copyFrom(rangeEnd.getBytes()))
                        .setPermType(type)
                        .build();
        AuthRoleGrantPermissionRequest req =
                AuthRoleGrantPermissionRequest.newBuilder()
                        .setNameBytes(ByteString.copyFrom(role.getBytes()))
                        .setPerm(perm)
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(
                                RequestWithToken.newBuilder()
                                        .setAuthRoleGrantPermissionRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleGrantPermissionResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleGetResponse> roleGet(ByteSequence role) {
        requireNonNull(role, "role can't be null");

        AuthRoleGetRequest req =
                AuthRoleGetRequest.newBuilder()
                        .setRoleBytes(ByteString.copyFrom(role.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthRoleGetRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleGetResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleListResponse> roleList() {
        AuthRoleListRequest req = AuthRoleListRequest.getDefaultInstance();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthRoleListRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleListResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleRevokePermissionResponse> roleRevokePermission(
            ByteSequence role, ByteSequence key, ByteSequence rangeEnd) {
        requireNonNull(role, "role can't be null");
        requireNonNull(key, "key can't be null");
        requireNonNull(rangeEnd, "rangeEnd can't be null");

        AuthRoleRevokePermissionRequest req =
                AuthRoleRevokePermissionRequest.newBuilder()
                        .setRoleBytes(ByteString.copyFrom(role.getBytes()))
                        .setKey(ByteString.copyFrom(key.getBytes()))
                        .setRangeEnd(ByteString.copyFrom(rangeEnd.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(
                                RequestWithToken.newBuilder()
                                        .setAuthRoleRevokePermissionRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleRevokePermissionResponse::new);
    }

    @Override
    public CompletableFuture<AuthRoleDeleteResponse> roleDelete(ByteSequence role) {
        requireNonNull(role, "role can't be null");

        AuthRoleDeleteRequest req =
                AuthRoleDeleteRequest.newBuilder()
                        .setRoleBytes(ByteString.copyFrom(role.getBytes()))
                        .build();
        Command command =
                Command.newBuilder()
                        .setRequest(RequestWithToken.newBuilder().setAuthRoleDeleteRequest(req))
                        .build();
        return protocolClient.propose(command, true, AuthRoleDeleteResponse::new);
    }
}
