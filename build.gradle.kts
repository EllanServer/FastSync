plugins {
    java
}

import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.jar.JarFile
import java.util.jar.JarOutputStream
import java.util.zip.ZipEntry
import org.gradle.api.file.DuplicatesStrategy

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
// Pattern: NO shade. The JAR is built with the runtime classpath, but
// those classes are NOT bundled inside the JAR. Instead, the runtimeOnly
// artifacts are copied to build/libs/ via the `copyRuntimeLibs` task.
// Paper's plugin loader picks up the lib/ folder automatically.
//
// This avoids relocation conflicts when multiple plugins depend on
// different versions of the same library (e.g., two plugins using
// different HikariCP versions no longer classpath-clash).
// =============================================================================
dependencies {
    // Compile-only: provided by the server at runtime
    compileOnly("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")
    compileOnly("com.velocitypowered:velocity-api:3.4.0-SNAPSHOT")
    compileOnly("org.slf4j:slf4j-api:2.0.13")
    compileOnly("io.netty:netty-bom:4.1.115.Final")
    compileOnly("io.netty:netty-transport:4.1.115.Final")
    compileOnly("io.netty:netty-buffer:4.1.115.Final")
    compileOnly("io.netty:netty-codec:4.1.115.Final")
    compileOnly("io.netty:netty-resolver:4.1.115.Final")

    // Implementation: these classes are referenced from main code, so they
    // must be on compileClasspath. The `jar` task excludes them from the
    // main JAR and the `copyRuntimeLibs` task copies them to a `lib/`
    // folder next to the main JAR, which is Paper's standard dependency
    // pattern (no shade, no relocation).
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.lz4:lz4-java:1.8.0")
    implementation("com.mysql:mysql-connector-j:9.0.0")
    implementation("io.lettuce:lettuce-core:6.4.0.RELEASE")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.3")
    implementation("org.reactivestreams:reactive-streams:1.0.4")
    // Sparrow libraries ship with FastSync in the lib/ folder. They are not
    // part of a vanilla Paper server, so they must be runtime dependencies.
    implementation("net.momirealms:sparrow-nbt:0.18.8")
    implementation("net.momirealms:sparrow-yaml:1.0.7")
    implementation("net.momirealms:sparrow-redis-message-broker:0.0.7")

    // Test
    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:1.20.1")
    testImplementation("org.testcontainers:mysql:1.20.1")
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    // Paper API must be on the test *runtime* classpath too, because
    // ConfigManager (and other production classes loaded by tests) reference
    // JavaPlugin directly. testCompileOnly caused ClassNotFoundException at test time.
    testImplementation("io.papermc.paper:paper-api:${paperVersion}-R0.1-SNAPSHOT")
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
//
//   ./gradlew test           - run unit + integration tests
//   ./gradlew build          - compile + test + assemble (both JARs + libs)
//   ./gradlew ci             - clean + build + verify (full CI pipeline)
//
// Visible via `./gradlew tasks --group=ci`, `--group=build`, `--group=test`.
// =============================================================================

// `build` task: keep Gradle's default assemble + test, ensure velocityJar
// is included and copyRuntimeLibs runs at assemble time.
tasks.named("build") {
    group = "build"
    description = "Compiles, tests, and assembles both Paper/Folia and Velocity JARs + lib/."
}

// `ci` task: full pipeline for CI systems (GitHub Actions, Jenkins, etc.)
val ci = tasks.register("ci") {
    group = "ci"
    description = "Full CI pipeline: clean, build, and test."
    dependsOn("clean", "build", "check")
}

// `testGroup`: explicit alias in the `test` group for clarity in CI logs
val testGroup = tasks.register("testGroup") {
    group = "test"
    description = "Runs unit and integration tests (alias for the standard 'test' task)."
    dependsOn("test")
}

// =============================================================================
// Velocity proxy module (custom source set + jar task)
// =============================================================================
// `runtimeOnly` is also non-resolvable by design (it's consumed at runtime,
// not at compile time). The source set's compile classpath already extends
// from `main` and inherits its `compileOnly` dependencies automatically, so we
// only need to add the main source set's compiled output here.
sourceSets {
    create("velocity") {
        java {
            srcDir("src/velocity/java")
        }
        resources {
            srcDir("src/velocity/resources")
        }
        // Pull `compileOnly` deps from the main configuration into the velocity
        // source set's compile classpath. We use `get()` (lazy) and
        // `incoming.resolutionResult` to avoid resolving the non-resolvable
        // `compileOnly` configuration directly.
        compileClasspath += sourceSets["main"].output
        runtimeClasspath += sourceSets["main"].output
    }
}

// `processVelocityResources` inherits the main source set's output by default
// (which carries `plugin.yml`). The main source set is rebuilt every time, so
// the same `plugin.yml` shows up as a duplicate in the velocity resource
// destination. Exclude it here — the Paper plugin descriptor is consumed only
// by the main `jar` task.
tasks.named<Copy>("processVelocityResources") {
    exclude("plugin.yml")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Surface `compileOnly` (which holds `velocity-api`, `slf4j-api`, `netty-*`,
// `sparrow-*`) to the velocity source set's compile classpath. The dependency
// `velocityOnly` block is consumed by the velocity source set's `velocityCompileClasspath`
// configuration but never enters the main compile classpath.
val velocityOnly by configurations.creating
dependencies {
    add("velocityOnly", "com.velocitypowered:velocity-api:3.4.0-SNAPSHOT")
    add("velocityOnly", "org.slf4j:slf4j-api:2.0.13")
    add("velocityOnly", "io.netty:netty-bom:4.1.115.Final")
    add("velocityOnly", "io.netty:netty-transport:4.1.115.Final")
    add("velocityOnly", "io.netty:netty-buffer:4.1.115.Final")
    add("velocityOnly", "io.netty:netty-codec:4.1.115.Final")
    add("velocityOnly", "io.netty:netty-resolver:4.1.115.Final")
}
sourceSets["velocity"].compileClasspath += velocityOnly

// Manually register the velocityJar task (Gradle's auto-jar is only created
// for source sets that exist at the time the `java` plugin is applied).
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
// Main JAR: Paper/Folia backend plugin (NO shade — lib/ folder pattern)
// =============================================================================
val generateClasspathListing = tasks.register("generateClasspathListing") {
    group = "build"
    description = "Builds a Class-Path manifest string from the copied runtime libs."
    val outFile = layout.buildDirectory.file("classpath.txt")
    outputs.file(outFile)
    dependsOn("copyRuntimeLibs")
    doLast {
        val libsDir = layout.buildDirectory.dir("libs").get().asFile
        val libNames = libsDir.listFiles { f -> f.isFile && f.name.endsWith(".jar") }
            ?.filter { it.name != "FastSync-${project.version}.jar" && it.name != "FastSync-Proxy-${project.version}.jar" }
            ?.sortedBy { it.name }
            ?.joinToString(" ") { "lib/${it.name}" }
            ?: ""
        outFile.get().asFile.writeText(libNames)
    }
}

tasks.jar {
    group = "build"
    description = "Packages the Paper/Folia backend plugin JAR (no shade — uses lib/ folder)."
    archiveBaseName.set("FastSync")
    archiveVersion.set(version.toString())
    dependsOn(generateClasspathListing)

    // Strip META-INF signatures that get rejected on reload
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA",
            "META-INF/maven/**", "META-INF/LICENSE*", "META-INF/NOTICE*")

    manifest {
        attributes(
            "Implementation-Title" to "FastSync",
            "Implementation-Version" to project.version.toString(),
            "Multi-Release" to "true"
        )
    }

    // After the JAR is built, inject the Class-Path entry that points at
    // the lib/ folder. We read the listing from generateClasspathListing
    // and update the manifest in-place using the standard Java JAR API.
    doLast {
        val classpath = generateClasspathListing.get().outputs.files.singleFile.readText()
        val manifestFile = archiveFile.get().asFile
        if (classpath.isBlank()) return@doLast

        val originalManifest = JarFile(manifestFile).use { it.manifest }
        originalManifest.mainAttributes.putValue("Class-Path", classpath)

        val tmp = File(manifestFile.parentFile, manifestFile.name + ".tmp")
        JarFile(manifestFile).use { src ->
            JarOutputStream(FileOutputStream(tmp), originalManifest).use { dst ->
                val entries = src.entries()
                while (entries.hasMoreElements()) {
                    val entry = entries.nextElement()
                    if (entry.name == "META-INF/MANIFEST.MF") continue
                    dst.putNextEntry(ZipEntry(entry.name))
                    src.getInputStream(entry).use { input ->
                        input.copyTo(dst)
                    }
                    dst.closeEntry()
                }
            }
        }
        Files.move(tmp.toPath(), manifestFile.toPath(),
            StandardCopyOption.REPLACE_EXISTING)
    }
}

// =============================================================================
// Copy runtime dependencies to build/libs (for distribution convenience)
// =============================================================================
val copyRuntimeLibs = tasks.register<Copy>("copyRuntimeLibs") {
    group = "build"
    description = "Copies runtime dependencies to build/libs for distribution."
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("libs"))
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE

    // Composite builds (Sparrow submodules) may produce jars with identical
    // base filenames (e.g. core.jar). Rename duplicates so both make it into
    // the lib/ folder and the Class-Path manifest.
    val seen = mutableSetOf<String>()
    eachFile {
        var newName = name
        var counter = 1
        while (!seen.add(newName)) {
            newName = name.removeSuffix(".jar") + "-$counter.jar"
            counter++
        }
        name = newName
    }
}

// =============================================================================
// Fat JAR: bundles runtime dependencies (Paper does not load Class-Path libs)
// =============================================================================
// Paper's plugin remapper ignores the Class-Path manifest, so the lib/ folder
// pattern used above does not work at runtime. For environments that need a
// single deployable artifact, this task produces a fat jar with all runtime
// dependencies embedded (no relocation / no shade).
// =============================================================================
val fatJar = tasks.register<Jar>("fatJar") {
    group = "build"
    description = "Packages a standalone Paper/Folia plugin JAR with all runtime dependencies embedded (no shade)."
    archiveBaseName.set("FastSync-All")
    archiveVersion.set(version.toString())
    from(sourceSets["main"].output)
    from({
        configurations.runtimeClasspath.get().map { f ->
            if (f.isDirectory) f else zipTree(f)
        }
    })
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA",
            "META-INF/maven/**", "META-INF/LICENSE*", "META-INF/NOTICE*")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes(
            "Implementation-Title" to "FastSync",
            "Implementation-Version" to project.version.toString(),
            "Multi-Release" to "true"
        )
    }
}

tasks.named("assemble") {
    dependsOn(velocityJar, copyRuntimeLibs, fatJar)
}

tasks.named("check") {
    dependsOn("test")
}
