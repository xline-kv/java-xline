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

/** AuthDisableResponse returned by {@link Auth#authDisable()} contains a header. */
public class AuthDisableResponse extends AbstractResponse<com.xline.protobuf.AuthDisableResponse> {

    public AuthDisableResponse(com.xline.protobuf.AuthDisableResponse response) {
        super(response, response.getHeader());
    }

    public AuthDisableResponse(CommandResponse sr, SyncResponse asr) {
        super(
                sr,
                asr,
                CommandResponse::getAuthDisableResponse,
                com.xline.protobuf.AuthDisableResponse::getHeader);
    }
}
