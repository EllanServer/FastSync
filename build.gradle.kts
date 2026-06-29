import org.gradle.api.file.DuplicatesStrategy
import java.time.Duration

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
//   - Self-contained Maven Central libs (HikariCP, LZ4, jOOQ, Caffeine)
//     → implementation, shaded + relocated into the JAR to avoid cold-start
//     download dependency and classpath conflicts.
//   - Complex Maven Central libs (Redisson, MySQL connector, reactive-streams)
//     → compileOnly for compilation; declared in plugin.yml `libraries:` so
//     Paper downloads them at startup. These have deep transitive chains
//     (netty / jackson / protobuf) that make shading + relocating risky.
//   - Paper API → compileOnly (provided by the server).
// =============================================================================
dependencies {
    // Compile-only: provided by Paper/Folia at runtime
    compileOnly("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")

    // Velocity proxy API. Velocity publishes ONLY snapshot artifacts (no stable
    // releases) — 3.5.0-SNAPSHOT is the current version on PaperMC's Maven repo
    // and is the one shipped by the official Velocity download page. This is
    // compile-only: the proxy plugin is optional, and operators should test
    // against their production Velocity build. If API breaks occur, pin to a
    // specific build number (e.g. 3.5.0-SNAPSHOT-599).
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

    // Self-contained runtime deps: shaded into the JAR (implementation) so
    // the plugin does not depend on downloading them at cold start. Relocated
    // in shadowJar to avoid classpath conflicts with other plugins.
    implementation("com.zaxxer:HikariCP:7.1.0")
    implementation("at.yawk.lz4:lz4-java:1.11.0")
    implementation("org.jooq:jooq:3.21.6")
    // jOOQ marks JAXB annotations as optional, but its public class files use
    // them. Keep the API on the compile classpath so clean -Xlint builds can
    // inspect those annotations without emitting missing-class warnings.
    compileOnly("jakarta.xml.bind:jakarta.xml.bind-api:4.0.5")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.4")
    // jOOQ 3.21 has a transitive dependency on io.r2dbc:r2dbc-spi (jOOQ's R2DBC
    // support). jOOQ's static initializer references io.r2dbc.spi.ConnectionFactory
    // even when only the JDBC DSL is used, so r2dbc-spi MUST be on the runtime
    // classpath. We shade it in (NOT relocated — it is a pure SPI interface
    // package and jOOQ's reflection looks up the literal io.r2dbc.spi.* names).
    // Keeping it out of plugin.yml libraries avoids one more cold-start download.
    implementation("io.r2dbc:r2dbc-spi:1.0.0.RELEASE")

    // Complex runtime deps with deep transitive chains (netty / jackson /
    // protobuf): kept as compileOnly and declared in plugin.yml `libraries:`
    // so Paper downloads them at startup. Shading + relocating these reliably
    // is high-risk (Redisson ↔ netty reflection, MySQL ↔ protobuf bloat), so
    // they stay on the libraries path for production safety.
    compileOnly("com.mysql:mysql-connector-j:9.7.0")
    // Redis coordination: Redisson replaces Lettuce + sparrow-redis-message-broker.
    // Provides RFencedLock, RTopic (Pub/Sub), RStream (Streams) in one library.
    compileOnly("org.redisson:redisson:4.6.1")
    compileOnly("org.reactivestreams:reactive-streams:1.0.4")

    // Sparrow libraries: shaded into the JAR (not on Maven Central).
    // sparrow-redis-message-broker removed — replaced by Redisson.
    implementation("net.momirealms:sparrow-nbt:0.18.8")
    implementation("net.momirealms:sparrow-yaml:1.0.7")

    // Test
    testImplementation(platform("org.junit:junit-bom:6.1.1"))
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
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// Stress tests need Docker and several minutes, so keep them out of the
// default test task. The dedicated stressTest task below owns that package.
tasks.named<Test>("test") {
    filter {
        excludeTestsMatching("com.fastsync.stress.*")
    }
}

// Dedicated task to run only the stress tests (invoked by .github/workflows/stress.yml).
val stressTest = tasks.register<Test>("stressTest") {
    group = "verification"
    description = "Runs the @Tag(\"stress\") tests (requires Docker)."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    filter {
        includeTestsMatching("com.fastsync.stress.*")
    }
    // Stress tests are slow + need Docker; give them room.
    timeout.set(Duration.ofMinutes(30))
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

val velocityOnly = configurations.create("velocityOnly")
dependencies {
    add("velocityOnly", "com.velocitypowered:velocity-api:3.5.0-SNAPSHOT")
    add("velocityOnly", "org.slf4j:slf4j-api:2.0.18")
    // Adventure exposes JetBrains annotations in its public API but does not
    // publish them transitively for this custom compile-only configuration.
    add("velocityOnly", "org.jetbrains:annotations:26.1.0")
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
// Self-contained runtime deps (HikariCP, LZ4, jOOQ, Caffeine) + Sparrow
// libraries are shaded into this JAR and relocated to com.fastsync.libs.*.
// Complex deps (Redisson, MySQL connector, reactive-streams) are declared in
// plugin.yml `libraries:` and auto-downloaded by Paper at startup. The manifest
// declares `paperweight-mappings-namespace: mojang` so Paper skips its
// PluginRemapper.
tasks.shadowJar {
    group = "build"
    description = "Packages the Paper/Folia backend plugin JAR with runtime dependencies shaded + relocated."
    archiveBaseName.set("FastSync")
    archiveVersion.set(version.toString())
    archiveClassifier.set("")

    // Shade the self-contained runtime deps + Sparrow libraries. Complex deps
    // (Redisson, MySQL connector, reactive-streams) are excluded — they stay
    // in plugin.yml `libraries:` and are downloaded by Paper at startup.
    // NOTE: io.r2dbc is shaded WITHOUT relocation — jOOQ's static initializer
    // references io.r2dbc.spi.ConnectionFactory by literal class name, so the
    // package must keep its original io.r2dbc.* name.
    dependencies {
        exclude {
            val group = it.moduleGroup
            !group.startsWith("net.momirealms")
                && !group.startsWith("com.zaxxer")
                && !group.startsWith("at.yawk")
                && !group.startsWith("org.jooq")
                && !group.startsWith("com.github.ben-manes")
                && !group.startsWith("io.r2dbc")
        }
    }

    // Relocate shaded libraries to avoid classpath conflicts with other plugins.
    // io.r2dbc is intentionally NOT relocated — see comment above.
    relocate("com.zaxxer.hikari", "com.fastsync.libs.hikari")
    relocate("net.jpountz", "com.fastsync.libs.lz4")
    relocate("org.jooq", "com.fastsync.libs.jooq")
    relocate("com.github.benmanes.caffeine", "com.fastsync.libs.caffeine")

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
