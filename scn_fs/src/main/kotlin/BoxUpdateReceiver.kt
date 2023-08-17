package ru.itmo.scn.fs

import kotlinx.coroutines.channels.Channel
import java.nio.file.WatchEvent
import java.nio.file.Path
import io.ktor.application.*
import io.ktor.client.*
import io.ktor.client.engine.apache.*
import io.ktor.features.*
import io.ktor.gson.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.commandLineEnvironment
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

suspend fun boxUpdateReceiver( // boxDir:Path,
    //outChannel: Channel<Pair<Path, WatchEvent.Kind<Path>>>,
    otherArgs: Array<String>) {
        Log.info("Probably run fs_server")
        io.ktor.server.netty.EngineMain.main(otherArgs)
                    
}



@Suppress("unused") // Referenced in application.conf
@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    install(Compression) {
        gzip {
            priority = 1.0
        }
        deflate {
            priority = 10.0
            minimumSize(1024) // condition
        }
    }

    install(CallLogging) {
        level = Level.INFO
        filter { call -> call.request.path().startsWith("/") }
    }

    install(DefaultHeaders) {
        header("X-Engine", "Ktor") // will send this header with each response
    }

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
        }
    }

    val client = HttpClient(Apache) {
    }

    routing {

        install(ContentNegotiation) {
            gson {
                setDateFormat(DateFormat.LONG)
                setPrettyPrinting()
            }
        }

        route("scn_fs") {
            get("test") {
                call.respond("You are here!")
            }

        }

        install(StatusPages) {
            exception<AuthenticationException> {
                call.respond(HttpStatusCode.Unauthorized)
            }
            exception<AuthorizationException> {
                call.respond(HttpStatusCode.Forbidden)
            }

        }
    }
}

class AuthenticationException : RuntimeException()
class AuthorizationException : RuntimeException()