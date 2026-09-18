rootProject.name = "behovsakkumulator"

// Sett opp repositories basert på om vi kjører i CI eller ikke: i CI må vi autentisere mot
// GitHub Package Registry med en token, mens lokalt bruker vi Nav sitt interne Nexus-speil i stedet.
pluginManagement {
    repositories {
        if (providers.environmentVariable("GITHUB_ACTIONS").orNull == "true") {
            maven("https://maven.pkg.github.com/navikt/maven-release") {
                credentials {
                    username = "token"
                    password = providers.environmentVariable("GITHUB_TOKEN").orNull!!
                }
            }
        } else {
            maven("https://repo.adeo.no/repository/github-package-registry-navikt/")
        }
        gradlePluginPortal()
        mavenCentral()
    }
}
dependencyResolutionManagement {
    // Bare tillat repositories-oppsett her i settings.gradle.kts
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)

    repositories {
        if (providers.environmentVariable("GITHUB_ACTIONS").orNull == "true") {
            maven("https://maven.pkg.github.com/navikt/maven-release") {
                credentials {
                    username = "token"
                    password = providers.environmentVariable("GITHUB_TOKEN").orNull!!
                }
            }
        } else {
            maven("https://repo.adeo.no/repository/github-package-registry-navikt/")
        }
        mavenCentral()
    }
}
