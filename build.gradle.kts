plugins {
    kotlin("jvm") version "2.1.10"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    application
}

group = "org.jevy"
version = "0.1.0"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val kafkaVersion = "3.9.0"
val avroVersion = "1.12.0"
val confluentVersion = "7.8.0"
val googleSheetsVersion = "v4-rev20251110-2.0.0"
val googleAuthVersion = "1.30.1"
val okhttpVersion = "4.12.0"
val anthropicVersion = "2.14.0"
val logbackVersion = "1.5.16"
val slf4jVersion = "2.0.17"

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // Avro + Confluent Schema Registry serde
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")

    // Google Sheets API
    implementation("com.google.apis:google-api-services-sheets:$googleSheetsVersion")
    implementation("com.google.auth:google-auth-library-oauth2-http:$googleAuthVersion")

    // Anthropic SDK (Claude API)
    implementation("com.anthropic:anthropic-java:$anthropicVersion")

    // HTTP client (for Brave web search API)
    implementation("com.squareup.okhttp3:okhttp:$okhttpVersion")

    // JSON parsing (used by tools)
    implementation("com.google.code.gson:gson:2.12.1")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("io.mockk:mockk:1.13.14")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("org.jevy.tiller.categorizer.MainKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.jevy.tiller.categorizer.MainKt"
    }
}
