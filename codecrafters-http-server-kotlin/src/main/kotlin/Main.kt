import java.io.ByteArrayOutputStream
import java.io.File
import java.io.OutputStreamWriter
import java.io.Writer
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.Executors
import java.util.zip.GZIPOutputStream


private fun Socket.todoResponse() = output("HTTP/1.1 404 Not Found\r\n\r\n".toByteArray())

private fun Socket.output(response: ByteArray) = getOutputStream().use { it.write(response) }
private const val HTTP_VERSION = "HTTP/1.1"

private val CRLF = "\r\n".toByteArray()
private const val ECHO_PREFIX = "/echo/"
private const val FILES_PREFIX = "/files/"


private const val HEADER_CONTENT_TYPE_TEXT = "Content-Type: text/plain"
private const val HEADER_CONTENT_TYPE_OCTET_STREAM = "Content-Type: application/octet-stream"
private const val CONTENT_TYPE_OCTET = "application/octet-stream" // TODO refactor to not duplicate string

private fun httpResponse(
    httpCode: Int,
    body: ByteArray? = null,
    contentType: String? = null,
    headers: Map<String, String> = emptyMap()
): ByteArray {
    val buffer = ByteArrayOutputStream()
    buffer.write(HTTP_VERSION.toByteArray())
    buffer.write(" ".toByteArray())

    val statusLine = when (httpCode) {
        200 -> "200 OK"
        201 -> "201 Created"
        404 -> "404 Not Found"
        else -> {
            TODO("not supported: httpCode=$httpCode, body=$body") // TODO Not all codes are supported
        }
    }
    buffer.write(statusLine.toByteArray())
    buffer.write(CRLF)

    buffer.write((contentType ?: HEADER_CONTENT_TYPE_TEXT).toByteArray())
    buffer.write(CRLF)

    // Headers
    body?.let {
        buffer.write("Content-Length: ${it.size}".toByteArray())
        buffer.write(CRLF)
    }

    for ((headerKey, headerValue) in headers) {
        buffer.write("$headerKey: $headerValue".toByteArray())
        buffer.write(CRLF)
    }

    buffer.write(CRLF) // Marks the end of headers

    // Response body
    body?.let { buffer.write(it) }

    println("Response")
    println(">>>>>>>>")
    println(String(buffer.toByteArray()))
    println("<<<<<<<<")
    return buffer.toByteArray()
}

fun gzip(content: String): ByteArray {
    val bos = ByteArrayOutputStream()
    GZIPOutputStream(bos).bufferedWriter().use { it.write(content) }
    return bos.toByteArray()
}

fun main(arguments: Array<String>) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println("Logs from your program will appear here!")
    println("arguments=${arguments.toList()}")

    val path = if (arguments.getOrNull(0) == "--directory") {
        arguments.getOrNull(1)
    } else {
        null
    }

    val fileFolder = if (path != null) File(path) else File("./tmp/")
    println("fileFolder=$fileFolder")

    // Uncomment this block to pass the first stage
    val serverSocket = ServerSocket(4221)
    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    serverSocket.reuseAddress = true

    val threadExecutor = Executors.newFixedThreadPool(2)
    while (true) {
        val clientSocket = serverSocket.accept() // Wait for connection from client.
        // Note, accept is not necessarily thread safe.
        threadExecutor.execute {
            handleConnection(clientSocket, fileFolder)
        }
    }

    // threadExecutor.shutdown()
    // threadExecutor.awaitTermination(10_000, TimeUnit.SECONDS) // Not relevant if the tasks never end like ours do.
}

private fun handleConnection(clientSocket: Socket, fileFolder: File) {
    println("accepted new connection")

    val inputStream = clientSocket.getInputStream()
    // val lineContent = StringBuffer()
    val byteArray = ByteArray(4096)

    // TODO this is somehow truncating content in the first Concurrent connections test.
    // Temporary fix: read into a fixed size buffer.

    // Read while there's an indication that there's something to read.
    // However, 0 can be returned even if data becomes available later, only hints that there's nothing to read right now.
    // while (inputStream.available() != 0) {
    //     val amount = inputStream.read(byteArray)
    //     println("amount=$amount")
    //     if (amount <= -1) {
    //         break
    //     }
    //     lineContent.append(String(byteArray)) // TODO check if other encodings can break this?
    // }

    val readBytes = inputStream.read(byteArray)
    println("readBytes=$readBytes")
    val lineContent = String(byteArray)

    val lines = lineContent.split("\r\n")
    if (lines.isEmpty()) {
        clientSocket.todoResponse()
        return
    }

    for (line in lines) {
        println("| $line")
    }

    val requestLine = lines[0]
    println("requestLine=$requestLine")
    val requestLineParts = requestLine.split(" ")
    if (requestLineParts.size != 3) {
        clientSocket.todoResponse()
        return
    }
    val requestMethod = requestLineParts[0]
    val requestTarget = requestLineParts[1]
    val requestHttpVersion = requestLineParts[2]

    println("requestMethod=${requestMethod}")
    println("requestTarget=${requestTarget}")
    println("requestHttpVersion=${requestHttpVersion}")

    var bodyStartIndex = -1
    val headerLines = mutableListOf<String>()
    for (i in 1 until lines.size) {
        if (lines[i] == "") {
            bodyStartIndex =
                i + 1 // TODO our little hack to read the whole content is making this work but we should read the body according to content length and if it's really required.
            break
        } // Headers end with a double \r\n, so we we will an empty line here (list was split \r\n)
        headerLines.add(lines[i])
    }
    println("headerLines=$headerLines")

    val headers = headerLines.associate {
        val parts = it.split(": ", limit = 2)
        parts[0] to parts[1]
    }
    println("headers=$headers")

    val bodyContent = if (bodyStartIndex != -1) lines.subList(bodyStartIndex, lines.size).joinToString() else ""
    println("bodyContent=$bodyContent")

    when {
        requestTarget == "" || requestTarget == "/" -> clientSocket.output(httpResponse(200))
        requestTarget == "/user-agent" -> clientSocket.output(httpResponse(200, headers["User-Agent"]?.toByteArray()))

        requestTarget.startsWith(FILES_PREFIX) -> {
            val fileName = requestTarget.substring(FILES_PREFIX.length)
            val path = File(fileFolder, fileName)

            when (requestMethod) {
                "GET" -> {

                    if (path.exists() && path.isFile) {
                        clientSocket.output(
                            httpResponse(
                                200,
                                path.readBytes(),
                                contentType = HEADER_CONTENT_TYPE_OCTET_STREAM
                            )
                        )
                    } else {
                        clientSocket.output(httpResponse(404))
                    }
                }

                "POST" -> {
                    val contentType = headers["Content-Type"]
                    val contentLengthValue = headers["Content-Length"]
                    val contentLength = contentLengthValue?.toIntOrNull()

                    println("contentType=$contentType")
                    println("contentLengthValue=$contentLengthValue")
                    println("contentLength=$contentLength")

                    if (contentType == CONTENT_TYPE_OCTET && contentLength != null && contentLength > 0 && bodyContent.isNotEmpty()) {
                        val bodyContentToWrite = bodyContent.substring(0, contentLength)
                        println("path=$path, bodyContentToWrite=$bodyContentToWrite")
                        path.bufferedWriter().append(bodyContentToWrite).close()
                        clientSocket.output(httpResponse(201))
                    } else {
                        clientSocket.output(httpResponse(404))
                    }
                }

                else -> {
                    clientSocket.todoResponse()
                }
            }

        }

        requestTarget.startsWith(ECHO_PREFIX) -> {
            val contentToEcho = requestTarget.substring(ECHO_PREFIX.length)
            val acceptEncoding = headers["Accept-Encoding"]
            val acceptedEncodingTypes = acceptEncoding?.split(", ") ?: emptyList()

            val responseHeaders = mutableMapOf<String, String>()
            val body = if (acceptedEncodingTypes.isNotEmpty()) {
                if (acceptedEncodingTypes.contains("gzip")) {
                    responseHeaders["Content-Encoding"] = "gzip"
                    gzip(contentToEcho)
                } else {
                    // Requested compression but the encoding is not supported
                    // Return the body without any modifications and no extra header
                    contentToEcho.toByteArray()
                }
            } else {
                contentToEcho.toByteArray()
            }

            println("contentToEcho=$contentToEcho")
            println("acceptedEncodingTypes=$acceptedEncodingTypes")
            println("body=${String(body)}")

            clientSocket.output(httpResponse(200, body, headers = responseHeaders))
        }

        else -> {
            clientSocket.todoResponse()
        }
    }
}
