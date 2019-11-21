package no.nav.helse.sputnik

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.fullPath

fun fpsakMockClient(mockResponseGenerator: ResponseGenerator) = HttpClient(MockEngine) {
    engine {
        addHandler { request ->
            when {
                (request.url.fullPath).contains("/api/vedtak/gjeldendevedtak-foreldrepenger") -> {
                    respond(
                        mockResponseGenerator.foreldrepenger()
                    )
                }
                (request.url.fullPath).contains("/api/vedtak/gjeldendevedtak-svangerskapspenger") -> {
                    respond(
                        mockResponseGenerator.svangerskapspenger()
                    )
                }
                else -> error("Endepunktet finnes ikke ${request.url.fullPath}")
            }
        }

    }
}

interface ResponseGenerator {
    fun foreldrepenger() = foreldrepengerResponse()
    fun svangerskapspenger() = svangerskapspengerResponse()

}

fun svangerskapspengerResponse() = """
[
    {
        "version": "1.0",
        "aktør": {
            "verdi": "aktør"
        },
        "vedtattTidspunkt": "2019-10-18T00:00:00",
        "type": {
            "kode": "SVP",
            "kodeverk": "FAGSAK_YTELSE_TYPE"
        },
        "saksnummer": "140260023",
        "vedtakReferanse": "20e89c46-9956-4e8d-a0fb-174e079f331f",
        "status": {
            "kode": "LOP",
            "kodeverk": "YTELSE_STATUS"
        },
        "fagsystem": {
            "kode": "FPSAK",
            "kodeverk": "FAGSYSTEM"
        },
        "periode": {
            "fom": "2019-10-01",
            "tom": "2020-02-07"
        },
        "anvist": [
            {
                "periode": {
                    "fom": "2019-10-01",
                    "tom": "2020-02-07"
                },
                "beløp": null,
                "dagsats": null,
                "utbetalingsgrad": null
            }
        ]
    }
]
"""

fun foreldrepengerResponse() = """
[
    {
        "version": "1.0",
        "aktør": {
            "verdi": "aktør"
        },
        "vedtattTidspunkt": "2019-10-18T00:00:00",
        "type": {
            "kode": "SVP",
            "kodeverk": "FAGSAK_YTELSE_TYPE"
        },
        "saksnummer": "140260023",
        "vedtakReferanse": "20e89c46-9956-4e8d-a0fb-174e079f331f",
        "status": {
            "kode": "LOP",
            "kodeverk": "YTELSE_STATUS"
        },
        "fagsystem": {
            "kode": "FPSAK",
            "kodeverk": "FAGSYSTEM"
        },
        "periode": {
            "fom": "2019-10-01",
            "tom": "2020-02-07"
        },
        "anvist": [
            {
                "periode": {
                    "fom": "2019-10-01",
                    "tom": "2020-02-07"
                },
                "beløp": null,
                "dagsats": null,
                "utbetalingsgrad": null
            }
        ]
    }
]
"""
