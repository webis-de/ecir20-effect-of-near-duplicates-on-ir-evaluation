plugins {
    id("java")
    id("maven-publish")
    id("io.freefair.lombok") version "4.1.4"
    id("com.github.johnrengelman.shadow") version "5.2.0"
}

group = "de.webis"
version = "1.0-SNAPSHOT"
description = "trec-ndd"

// Disable automatic tests, when building this project.
// This is temporary as we can't currently run the Spark cluster in Gradle yet.
tasks.test {
    enabled = false
}

repositories {
    jcenter()
    mavenCentral()
    maven(url = "https://jitpack.io")
    maven(url = "https://raw.githubusercontent.com/lintool/AnseriniMaven/master/mvn-repo/")
    maven(url = "lib")
}

dependencies {
    compile("net.sourceforge.argparse4j:argparse4j:0.8.1")
    compile("com.google.guava:guava:27.1-jre")
    compile("org.apache.spark:spark-graphx_2.12:2.4.3")
    compileOnly("org.apache.spark:spark-core_2.12:2.4.3")
    compile("org.slf4j:slf4j-log4j12:1.7.26")
    compile("com.jayway.jsonpath:json-path:2.4.0")
    compile("io.anserini:anserini:0.5.1")
    compile("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.6.7")
    compile("org.elasticsearch.client:elasticsearch-rest-client:6.7.1")
    compile("client.netspeak:netspeak-client:1.3.5")
    compile("net.jodah:failsafe:2.0.1")
    compile("org.apache.solr:solr-core:8.1.1")
    compile("com.github.mam10eks:jtreceval:-SNAPSHOT")
    testCompile("junit:junit:4.11")
    testCompile("com.approvaltests:approvaltests:3.2.0")
    testCompile("org.powermock:powermock-api-mockito:1.7.4")
    testCompile("org.powermock:powermock-core:1.7.4")
    testCompile("org.powermock:powermock-module-junit4:2.0.2")
    testCompile("com.google.code.gson:gson:2.8.5")
    testCompile("com.holdenkarau:spark-testing-base_2.12:2.4.3_0.12.0")
}

tasks.shadowJar {
    dependencies {
        // Dependencies from https://jitpack.io
        include(dependency("com.github.mam10eks:jtreceval"))
        include(dependency("com.github.TREMA-UNH:trec-car-tools-java"))
        // Dependencies from https://raw.githubusercontent.com/lintool/AnseriniMaven/master/mvn-repo/
        include(dependency("io.anserini:anserini"))
        // Dependencies from lib
        include(dependency("client.netspeak:netspeak-client"))
        include(dependency("com.linkedin:scanns"))
    }
}

lombok {
    version.set("1.18.10")
}

publishing {
    publications {
        register<MavenPublication>("shadow") {
            artifact(tasks.shadowJar.get()) {
                classifier = null
            }
        }
    }
}

tasks.withType(JavaCompile::class) {
    options.encoding = "UTF-8"
    sourceCompatibility = "1.8"
}
