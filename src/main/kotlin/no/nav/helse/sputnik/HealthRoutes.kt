package no.nav.helse.sputnik

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respondText
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.exporter.common.TextFormat

private val prometheusContentType = ContentType.parse(TextFormat.CONTENT_TYPE_004)

fun Routing.registerHealthApi(
    liveness: () -> Boolean,
    readiness: () -> Boolean,
    meterRegistry: PrometheusMeterRegistry
) {
    get("/is_alive") {
        if (liveness()) {
            call.respondText("гудок")
        } else {
            call.respondText("я покойник", status = HttpStatusCode.InternalServerError)
        }
    }

    get("/is_ready") {
        if (readiness()) {
            call.respondText("гудок")
        } else {
            call.respondText("возможно подождать", status = HttpStatusCode.InternalServerError)
        }
    }

    get("/metrics") {
        call.respondText(
            text = meterRegistry.scrape(),
            contentType = prometheusContentType
        )
    }
}
