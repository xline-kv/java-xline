import com.adarshr.gradle.testlogger.TestLoggerExtension
import com.adarshr.gradle.testlogger.theme.ThemeType

group = "cloud.xline"
version = "1.0-SNAPSHOT"

buildscript {
    repositories {
        google()
        gradlePluginPortal()
    }

    dependencies {
        classpath("org.gradle:test-retry-gradle-plugin:${libs.versions.testRetryPlugin.get()}")
        classpath("com.adarshr:gradle-test-logger-plugin:${libs.versions.testLoggerPlugin.get()}")
    }
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "org.gradle.test-retry")
    apply(plugin = "com.adarshr.test-logger")

    tasks {
        named<JavaCompile>("compileJava") {
            // Keep same java with jetcd
            options.release = 11
        }

        named<Test>("test") {
            useJUnitPlatform()

            maxParallelForks = Runtime.getRuntime().availableProcessors()

            // Keep same with jetcd
            retry {
                maxRetries = 1
                maxFailures = 5
            }
        }
    }

    extensions.getByType<TestLoggerExtension>().apply {
        theme = ThemeType.MOCHA_PARALLEL
        showStandardStreams = false
    }
}
