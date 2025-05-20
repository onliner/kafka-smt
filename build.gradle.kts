group = "org.onliner.kafka.smt"
version = System.getenv("VERSION") ?: "1.0.0"

val javaVersion = 17

val artifactoryContext =
    project.properties.getOrDefault("artifactory_context", System.getenv("ARTIFACTORY_CONTEXT")).toString()
val artifactoryUsername =
    project.properties.getOrDefault("artifactory_user", System.getenv("ARTIFACTORY_USER")).toString()
val artifactoryPassword =
    project.properties.getOrDefault("artifactory_password", System.getenv("ARTIFACTORY_PWD")).toString()

plugins {
    kotlin("jvm") version "2.1.21"
    idea
    `java-library`
    `maven-publish`
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenCentral()
}

dependencies {
    val kafkaConnectVersion = "3.9.+"
    val junitVersion = "5.8.2"

    compileOnly(platform("org.jetbrains.kotlin:kotlin-bom"))
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    compileOnly("org.apache.kafka:connect-api:$kafkaConnectVersion")
    compileOnly("org.apache.kafka:connect-transforms:${kafkaConnectVersion}")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.4")
    implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")

    testImplementation("org.apache.kafka:connect-api:${kafkaConnectVersion}")
    testImplementation("org.apache.kafka:connect-transforms:${kafkaConnectVersion}")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
}

kotlin {
    jvmToolchain(javaVersion)
}

tasks.shadowJar {
    minimize()

    archiveBaseName.set(project.name)
    archiveVersion.set(project.version.toString())
    archiveClassifier.set("")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            pom {
                name.set("onliner-kafka-smt")
                description.set( project.description)
                url.set("https://github.com/onliner/kafka-smt")
                organization {
                    name.set("Onliner")
                    url.set("https://github.com/onliner")
                }
                issueManagement {
                    system.set("GitHub")
                    url.set("https://github.com/onliner/kafka-smt/issues")
                }
                licenses {
                    license {
                        name.set( "Apache License 2.0")
                        url.set("https://github.com/onliner/kafka-smt/blob/master/LICENSE")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        name.set("onliner")
                    }
                }
                scm {
                    url.set("https://github.com/onliner/kafka-smt")
                    connection.set("scm:git:git://github.com/onliner/kafka-smt.git")
                    developerConnection.set("scm:git:ssh://git@github.com:onliner/kafka-smt.git")
                }
            }

            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ArtifactoryLocal"
            url = uri(artifactoryContext + "/libs-release-local")
            credentials {
                username = artifactoryUsername
                password = artifactoryPassword
            }
        }
    }
}

