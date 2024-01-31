dependencies {
    api(project(":jxline-proto"))
    api(libs.jetcd)

    testImplementation(libs.bundles.testing)
}
