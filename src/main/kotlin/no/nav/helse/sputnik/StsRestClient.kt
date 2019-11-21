package no.nav.helse.sputnik

import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.HttpClient
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.response.HttpResponse
import io.ktor.client.response.readText
import io.ktor.http.ContentType
import kotlinx.coroutines.runBlocking
import java.time.LocalDateTime

/**
 * henter jwt token fra STS
 */
class StsRestClient(private val baseUrl: String, private val httpClient: HttpClient) {
    private var cachedOidcToken: Token? = null

    fun token(): String {
        if (Token.shouldRenew(cachedOidcToken)) {
            runBlocking {
                val response =
                    httpClient.get<HttpResponse>(
                        "$baseUrl/rest/v1/sts/token?grant_type=client_credentials&scope=openid"
                    ) {
                        accept(ContentType.Application.Json)
                    }


                val parsedResponse = objectMapper.readValue<TokenResponse>(response.readText())

                val token = Token(
                    accessToken = parsedResponse.access_token,
                    type = parsedResponse.token_type,
                    expiresIn = parsedResponse.expires_in
                )

                cachedOidcToken = token
            }
        }

        return cachedOidcToken!!.accessToken
    }

    data class TokenResponse(val access_token: String, val token_type: String, val expires_in: Int)

    data class Token(val accessToken: String, val type: String, val expiresIn: Int) {
        // expire 10 seconds before actual expiry. for great margins.
        val expirationTime: LocalDateTime = LocalDateTime.now().plusSeconds(expiresIn - 10L)

        companion object {
            fun shouldRenew(token: Token?): Boolean = if (token == null) true else isExpired(token)


            private fun isExpired(token: Token): Boolean = token.expirationTime.isBefore(LocalDateTime.now())

        }
    }
}
