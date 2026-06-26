import org.gradle.api.file.DuplicatesStrategy
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
    maven("https://oss.sonatype.org/content/repositories/snapshots/")
    maven("https://jitpack.io")
    // Sparrow 系列库（sparrow-nbt / sparrow-yaml / sparrow-redis-message-broker）
    // 由 Xiao-MoMi 维护并发布到此仓库。
    maven("https://repo.momirealms.net/releases/")
}

group = property("group") as String
version = property("version") as String
val paperVersion: String = property("paper.version") as String

// =============================================================================
// Dependencies
// =============================================================================
//
// Strategy:
//   - Sparrow libraries (not on Maven Central) → implementation, shaded into
//     the JAR by the shadowJar task. No relocation needed since FastSync is
//     the only consumer.
//   - Maven Central libraries (HikariCP, Lettuce, Caffeine, etc.) → compileOnly
//     for compilation; declared in plugin.yml `libraries:` so Paper downloads
//     them automatically at startup.
//   - Paper API → compileOnly (provided by the server).
// =============================================================================
dependencies {
    // Compile-only: provided by Paper/Folia at runtime
    compileOnly("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")

    // Velocity proxy API
    compileOnly("com.velocitypowered:velocity-api:3.4.0-SNAPSHOT")
    compileOnly("org.slf4j:slf4j-api:2.0.13")
    compileOnly("io.netty:netty-bom:4.2.15.Final")
    compileOnly("io.netty:netty-transport:4.2.15.Final")
    compileOnly("io.netty:netty-buffer:4.2.15.Final")
    compileOnly("io.netty:netty-codec:4.2.15.Final")
    compileOnly("io.netty:netty-resolver:4.2.15.Final")

    // Maven Central runtime deps: compileOnly because Paper will download them
    // via plugin.yml `libraries:` at startup.
    compileOnly("com.zaxxer:HikariCP:5.1.0")
    compileOnly("org.lz4:lz4-java:1.8.0")
    compileOnly("com.mysql:mysql-connector-j:9.0.0")
    compileOnly("io.lettuce:lettuce-core:6.4.0.RELEASE")
    compileOnly("com.github.ben-manes.caffeine:caffeine:3.2.3")
    compileOnly("org.reactivestreams:reactive-streams:1.0.4")

    // Sparrow libraries: shaded into the JAR (not on Maven Central, so Paper
    // cannot auto-download them).
    implementation("net.momirealms:sparrow-nbt:0.18.8")
    implementation("net.momirealms:sparrow-yaml:1.0.7")
    implementation("net.momirealms:sparrow-redis-message-broker:0.0.7")

    // Test
    testImplementation(platform("org.junit:junit-bom:6.1.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:mysql:2.0.5")
    testImplementation("org.testcontainers:junit-jupiter:2.0.5")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.23.0")
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    // Paper API + Maven Central runtime deps needed at test runtime.
    testImplementation("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")
    testImplementation("com.zaxxer:HikariCP:5.1.0")
    testImplementation("org.lz4:lz4-java:1.8.0")
    testImplementation("com.mysql:mysql-connector-j:9.0.0")
    testImplementation("io.lettuce:lettuce-core:6.4.0.RELEASE")
    testImplementation("com.github.ben-manes.caffeine:caffeine:3.2.3")
    testImplementation("org.reactivestreams:reactive-streams:1.0.4")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(21)
    options.compilerArgs.addAll(listOf(
        "-Xlint:all",
        "-Xlint:-serial",
        "-Xlint:-processing"
    ))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// =============================================================================
// Task groups: ci / build / test
// =============================================================================

tasks.named("build") {
    group = "build"
    description = "Compiles, tests, and assembles both Paper/Folia and Velocity JARs."
}

val ci = tasks.register("ci") {
    group = "ci"
    description = "Full CI pipeline: clean, build, and test."
    dependsOn("clean", "build", "check")
}

val testGroup = tasks.register("testGroup") {
    group = "test"
    description = "Runs unit and integration tests (alias for the standard 'test' task)."
    dependsOn("test")
}

// =============================================================================
// Velocity proxy module (custom source set + jar task)
// =============================================================================
sourceSets {
    create("velocity") {
        java {
            srcDir("src/velocity/java")
        }
        resources {
            srcDir("src/velocity/resources")
        }
        compileClasspath += sourceSets["main"].output
        runtimeClasspath += sourceSets["main"].output
    }
}

tasks.named<Copy>("processVelocityResources") {
    exclude("plugin.yml")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val velocityOnly by configurations.creating
dependencies {
    add("velocityOnly", "com.velocitypowered:velocity-api:3.4.0-SNAPSHOT")
    add("velocityOnly", "org.slf4j:slf4j-api:2.0.13")
    add("velocityOnly", "io.netty:netty-bom:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-transport:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-buffer:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-codec:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-resolver:4.2.15.Final")
    // SnakeYAML for proxy-config.yml parsing (Velocity bundles it at runtime)
    add("velocityOnly", "org.yaml:snakeyaml:2.2")
}
sourceSets["velocity"].compileClasspath += velocityOnly

val velocityJar = tasks.register<Jar>("velocityJar") {
    group = "build"
    description = "Packages the Velocity proxy plugin JAR."
    archiveBaseName.set("FastSync-Proxy")
    archiveVersion.set(version.toString())
    from(sourceSets["velocity"].output)
    manifest {
        attributes(
            "Main-Class" to "com.fastsync.velocity.FastSyncProxy",
            "Implementation-Title" to "FastSync Proxy",
            "Implementation-Version" to project.version.toString()
        )
    }
}

// =============================================================================
// Main JAR: Paper/Folia backend plugin
// =============================================================================
// Sparrow libraries are shaded into this JAR (no relocation). Maven Central
// dependencies are declared in plugin.yml `libraries:` and auto-downloaded by
// Paper at startup. The manifest declares `paperweight-mappings-namespace:
// mojang` so Paper skips its PluginRemapper.
tasks.named<ShadowJar>("shadowJar") {
    group = "build"
    description = "Packages the Paper/Folia backend plugin JAR with Sparrow libraries shaded in."
    archiveBaseName.set("FastSync")
    archiveVersion.set(version.toString())
    archiveClassifier.set("")

    // Only shade Sparrow libraries; exclude everything else that might leak
    // into runtimeClasspath (e.g. transitive deps of Sparrow that are also on
    // Maven Central and declared in plugin.yml libraries).
    dependencies {
        include(dependency("net.momirealms:sparrow-nbt:"))
        include(dependency("net.momirealms:sparrow-yaml:"))
        include(dependency("net.momirealms:sparrow-redis-message-broker:"))
    }

    // Strip META-INF signatures and duplicate metadata
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA",
            "META-INF/maven/**", "META-INF/LICENSE*", "META-INF/NOTICE*")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes(
            "Implementation-Title" to "FastSync",
            "Implementation-Version" to project.version.toString(),
            "Multi-Release" to "true",
            "paperweight-mappings-namespace" to "mojang"
        )
    }
}

// Disable the plain jar task — shadowJar is the main artifact.
tasks.named("jar") {
    enabled = false
}

tasks.named("assemble") {
    dependsOn("shadowJar", velocityJar)
}

tasks.named("check") {
    dependsOn("test")
}
