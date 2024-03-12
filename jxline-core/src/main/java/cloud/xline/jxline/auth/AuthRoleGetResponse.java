/*
 * Copyright 2016-2021 The jetcd authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.xline.jxline.auth;

import cloud.xline.jxline.Auth;
import cloud.xline.jxline.impl.AbstractResponse;
import com.xline.protobuf.CommandResponse;
import com.xline.protobuf.SyncResponse;
import io.etcd.jetcd.ByteSequence;

import java.util.List;
import java.util.stream.Collectors;

/**
 * AuthRoleGetResponse returned by {@link Auth#roleGet(ByteSequence)} contains a header and a list
 * of permissions.
 */
public class AuthRoleGetResponse extends AbstractResponse<com.xline.protobuf.AuthRoleGetResponse> {

    private List<Permission> permissions;

    public AuthRoleGetResponse(com.xline.protobuf.AuthRoleGetResponse response) {
        super(response, response.getHeader());
    }

    public AuthRoleGetResponse(CommandResponse sr, SyncResponse asr) {
        super(
                sr,
                asr,
                CommandResponse::getAuthRoleGetResponse,
                com.xline.protobuf.AuthRoleGetResponse::getHeader);
    }

    private static Permission toPermission(com.xline.protobuf.Permission perm) {
        ByteSequence key = ByteSequence.from(perm.getKey());
        ByteSequence rangeEnd = ByteSequence.from(perm.getRangeEnd());

        Permission.Type type;
        switch (perm.getPermType()) {
            case READ:
                type = Permission.Type.READ;
                break;
            case WRITE:
                type = Permission.Type.WRITE;
                break;
            case READWRITE:
                type = Permission.Type.READWRITE;
                break;
            default:
                type = Permission.Type.UNRECOGNIZED;
        }

        return new Permission(type, key, rangeEnd);
    }

    public synchronized List<Permission> getPermissions() {
        if (permissions == null) {
            permissions =
                    getResponse().getPermList().stream()
                            .map(AuthRoleGetResponse::toPermission)
                            .collect(Collectors.toList());
        }

        return permissions;
    }
}
