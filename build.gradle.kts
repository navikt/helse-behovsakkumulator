val junitJupiterVersion = "5.6.2"
val ktorVersion = "1.3.2"

plugins {
    kotlin("jvm") version "1.3.72"
}

group = "no.nav.helse"

repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
    maven("https://kotlin.bintray.com/ktor")
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:1.2954646")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-auth-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-json-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-jackson:$ktorVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("no.nav:kafka-embedded-env:2.4.0")
    testImplementation("org.awaitility:awaitility:4.0.3")

    testImplementation("io.ktor:ktor-client-mock-jvm:$ktorVersion")
    testImplementation("io.mockk:mockk:1.10.0")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "12"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "12"
    }

    named<Jar>("jar") {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.behovsakkumulator.AppKt"
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("$buildDir/libs/${it.name}")
                if (!file.exists())
                    it.copyTo(file)
            }
        }
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}
