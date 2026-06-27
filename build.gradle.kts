import org.gradle.api.file.DuplicatesStrategy

plugins {
    java
    id("com.gradleup.shadow") version "9.4.3"
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
    // Velocity 3.5.0-SNAPSHOT + paper-api snapshots resolve from here. Note
    // this is a development snapshot; if you pin Velocity to a stable release
    // you can likely drop this repository.
    maven("https://oss.sonatype.org/content/repositories/snapshots/")
    // Sparrow 系列库（sparrow-nbt / sparrow-yaml）
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
//   - Sparrow-NBT / Sparrow-YAML (not on Maven Central) → implementation,
//     shaded into the JAR by the shadowJar task.
//   - Maven Central libraries (Redisson, jOOQ, HikariCP, etc.)
//     → compileOnly for compilation; declared in plugin.yml `libraries:` so
//     Paper downloads them automatically at startup.
//   - Paper API → compileOnly (provided by the server).
// =============================================================================
dependencies {
    // Compile-only: provided by Paper/Folia at runtime
    compileOnly("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")

    // Velocity proxy API
    compileOnly("com.velocitypowered:velocity-api:3.5.0-SNAPSHOT")
    compileOnly("org.slf4j:slf4j-api:2.0.18")
    // Netty BOM must be declared with platform(...) so Gradle treats it as a
    // BOM (dependency constraints) rather than a regular dependency. Without
    // platform(), the BOM artifact itself gets added to the classpath as a
    // no-op jar, and version alignment of netty-* modules relies on the
    // explicit versions below rather than the BOM. Using platform() also
    // future-proofs: if we later drop the explicit versions, the BOM will
    // supply them correctly.
    compileOnly(platform("io.netty:netty-bom:4.2.15.Final"))
    compileOnly("io.netty:netty-transport:4.2.15.Final")
    compileOnly("io.netty:netty-buffer:4.2.15.Final")
    compileOnly("io.netty:netty-codec:4.2.15.Final")
    compileOnly("io.netty:netty-resolver:4.2.15.Final")

    // Maven Central runtime deps: compileOnly because Paper will download them
    // via plugin.yml `libraries:` at startup.
    compileOnly("com.zaxxer:HikariCP:7.1.0")
    compileOnly("at.yawk.lz4:lz4-java:1.11.0")
    compileOnly("com.mysql:mysql-connector-j:9.7.0")
    // Redis coordination: Redisson replaces Lettuce + sparrow-redis-message-broker.
    // Provides RFencedLock, RTopic (Pub/Sub), RStream (Streams) in one library.
    compileOnly("org.redisson:redisson:4.6.1")
    // Type-safe SQL DSL for OCC + fencing token CAS queries.
    compileOnly("org.jooq:jooq:3.21.6")
    compileOnly("com.github.ben-manes.caffeine:caffeine:3.2.4")
    compileOnly("org.reactivestreams:reactive-streams:1.0.4")

    // Sparrow libraries: shaded into the JAR (not on Maven Central).
    // sparrow-redis-message-broker removed — replaced by Redisson.
    implementation("net.momirealms:sparrow-nbt:0.18.8")
    implementation("net.momirealms:sparrow-yaml:1.0.7")

    // Test
    testImplementation(platform("org.junit:junit-bom:6.1.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    // Gradle 9.x no longer auto-adds the JUnit Platform launcher.
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Testcontainers 2.0: artifact names prefixed with "testcontainers-"
    // (e.g. "mysql" → "testcontainers-mysql"), and package relocated
    // (org.testcontainers.containers.MySQLContainer → org.testcontainers.mysql.MySQLContainer).
    testImplementation(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-mysql")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.slf4j:slf4j-simple:2.0.18")
    testImplementation("org.mockito:mockito-core:5.23.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.23.0")
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    // Paper API + Maven Central runtime deps needed at test runtime.
    testImplementation("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")
    testImplementation("com.zaxxer:HikariCP:7.1.0")
    testImplementation("at.yawk.lz4:lz4-java:1.11.0")
    testImplementation("com.mysql:mysql-connector-j:9.7.0")
    testImplementation("org.redisson:redisson:4.6.1")
    testImplementation("org.jooq:jooq:3.21.6")
    testImplementation("com.github.ben-manes.caffeine:caffeine:3.2.4")
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
    // Round 16: exclude `stress` tag from the default test task — these tests
    // need Docker + several minutes and are run by the dedicated stress.yml
    // workflow. The `stressTest` task below re-includes them.
    filter {
        excludeTestsMatching("com.fastsync.stress.*")
    }
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// Dedicated task to run only the stress tests (invoked by .github/workflows/stress.yml).
val stressTest = tasks.register<Test>("stressTest") {
    group = "verification"
    description = "Runs the @Tag(\"stress\") tests (requires Docker)."
    useJUnitPlatform()
    filter {
        includeTestsMatching("com.fastsync.stress.*")
    }
    // Stress tests are slow + need Docker; give them room.
    timeout.set(java.time.Duration.ofMinutes(30))
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
    add("velocityOnly", "com.velocitypowered:velocity-api:3.5.0-SNAPSHOT")
    add("velocityOnly", "org.slf4j:slf4j-api:2.0.18")
    // Netty BOM as platform (see main dependencies block for rationale)
    add("velocityOnly", platform("io.netty:netty-bom:4.2.15.Final"))
    add("velocityOnly", "io.netty:netty-transport:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-buffer:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-codec:4.2.15.Final")
    add("velocityOnly", "io.netty:netty-resolver:4.2.15.Final")
    // SnakeYAML for proxy-config.yml parsing (Velocity bundles it at runtime)
    add("velocityOnly", "org.yaml:snakeyaml:2.6")
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
// Main JAR: Paper/Folia backend plugin (shadowJar)
// =============================================================================
// Sparrow libraries are shaded into this JAR (no relocation). Maven Central
// dependencies are declared in plugin.yml `libraries:` and auto-downloaded by
// Paper at startup. The manifest declares `paperweight-mappings-namespace:
// mojang` so Paper skips its PluginRemapper.
tasks.shadowJar {
    group = "build"
    description = "Packages the Paper/Folia backend plugin JAR with Sparrow libraries shaded in."
    archiveBaseName.set("FastSync")
    archiveVersion.set(version.toString())
    archiveClassifier.set("")

    // Only shade Sparrow libraries (net.momirealms group).
    // All other runtimeClasspath deps (transitive deps of Sparrow, etc.)
    // are excluded — they're either on Maven Central (auto-downloaded by
    // Paper via plugin.yml libraries) or not needed at runtime.
    dependencies {
        exclude {
            it.moduleGroup != "net.momirealms"
        }
    }

    // Strip META-INF signatures and duplicate metadata
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA",
            "META-INF/maven/**", "META-INF/LICENSE*", "META-INF/NOTICE*")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes(
            "Implementation-Title" to "FastSync",
            "Implementation-Version" to project.version.toString(),
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
