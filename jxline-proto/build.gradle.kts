import com.google.protobuf.gradle.proto

plugins {
    alias(libs.plugins.protobuf)
}

dependencies {
    api(libs.slf4j)
    api(libs.vertxGrpc)
    api(libs.bundles.grpc)
    api(libs.bundles.javax)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
    }
    plugins {
        register("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
        }
        register("vertx") {
            artifact = "io.vertx:vertx-grpc-protoc-plugin:${libs.versions.vertx.get()}"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
                create("vertx")
            }
        }
    }
}

sourceSets {
    // main is hidden in `src/proto/curp-proto/src` and `src/proto/xline-proto/src`.
    // otherwise it will parse the directory structure, which will cause `generateProto`
    // task failed
    main {
        proto {
            srcDir("src/proto/curp-proto/src")
            srcDir("src/proto/xline-proto/src")
        }
    }
}
