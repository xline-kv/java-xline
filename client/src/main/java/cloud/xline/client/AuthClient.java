package cloud.xline.client;

import cloud.xline.client.exceptions.CommandExecutionException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xline.protobuf.*;

public class AuthClient {

    private final ProtocolClient curpClient;

    private final String token;

    public AuthClient(ProtocolClient curpClient, String token) {
        this.curpClient = curpClient;
        this.token = token;
    }

    public AuthEnableResponse authEnable() throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken req = RequestWithToken.newBuilder().setToken(this.token).setAuthEnableRequest(AuthEnableRequest.newBuilder().build()).build();
        Command cmd = Command.newBuilder().setRequest(req).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthEnableResponse();
    }

    public AuthDisableResponse authDisable() throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken req = RequestWithToken.newBuilder().setToken(this.token).setAuthDisableRequest(AuthDisableRequest.newBuilder().build()).build();
        Command cmd = Command.newBuilder().setRequest(req).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthDisableResponse();
    }

    public AuthStatusResponse authStatus() throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken req = RequestWithToken.newBuilder().setToken(this.token).setAuthStatusRequest(AuthStatusRequest.newBuilder().build()).build();
        Command cmd = Command.newBuilder().setRequest(req).build();
        CommandResponse resp = this.curpClient.propose(cmd, true);
        return resp.getAuthStatusResponse();
    }

    public AuthenticateResponse authenticate(AuthenticateRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthenticateRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthenticateResponse();
    }

    public AuthUserAddResponse userAdd(AuthUserAddRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserAddRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserAddResponse();
    }

    public AuthUserGetResponse userGet(AuthUserGetRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserGetRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserGetResponse();
    }

    public AuthUserListResponse userList(AuthRoleListRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleListRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, true);
        return resp.getAuthUserListResponse();
    }

    public AuthUserDeleteResponse userDelete(AuthUserDeleteRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserDeleteRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserDeleteResponse();
    }

    public AuthUserChangePasswordResponse userChangePassword(AuthUserChangePasswordRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserChangePasswordRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserChangePasswordResponse();
    }

    public AuthUserGrantRoleResponse userGrantRole(AuthUserGrantRoleRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserGrantRoleRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserGrantRoleResponse();
    }

    public AuthUserRevokeRoleResponse userRevokeRole(AuthUserRevokeRoleRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthUserRevokeRoleRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthUserRevokeRoleResponse();
    }

    public AuthRoleAddResponse roleAdd(AuthRoleAddRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleAddRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthRoleAddResponse();
    }

    public AuthRoleGetResponse roleGet(AuthRoleGetRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleGetRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthRoleGetResponse();
    }

    public AuthRoleListResponse roleList(AuthRoleListRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleListRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, true);
        return resp.getAuthRoleListResponse();
    }

    public AuthRoleDeleteResponse roleDelete(AuthRoleDeleteRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleDeleteRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthRoleDeleteResponse();
    }

    public AuthRoleGrantPermissionResponse roleGrantPermission(AuthRoleGrantPermissionRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleGrantPermissionRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthRoleGrantPermissionResponse();
    }

    public AuthRoleRevokePermissionResponse roleRevokePermission(AuthRoleRevokePermissionRequest req) throws InvalidProtocolBufferException, CommandExecutionException {
        RequestWithToken reqToken = RequestWithToken.newBuilder().setToken(this.token).setAuthRoleRevokePermissionRequest(req).build();
        Command cmd = Command.newBuilder().setRequest(reqToken).build();
        CommandResponse resp = this.curpClient.propose(cmd, false);
        return resp.getAuthRoleRevokePermissionResponse();
    }


}
