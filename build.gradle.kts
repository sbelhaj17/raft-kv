plugins {
    java
    application
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass = "raftkv.Main"
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("simulate") {
    description = "Runs the deterministic simulator over many seeds"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "raftkv.sim.Main"
}
