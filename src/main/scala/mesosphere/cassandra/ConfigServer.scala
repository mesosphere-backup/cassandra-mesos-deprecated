package mesosphere.cassandra

import org.eclipse.jetty.server.{Request, Server}
import org.eclipse.jetty.server.handler.AbstractHandler
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import java.io.{FileNotFoundException, File}
import scala.io.Source
import scala.collection.mutable
import java.net.URI

class ConfigServer(port: Int, cassConfigDir: String, seedNodes: mutable.Set[String]) extends Logger {

  val server = new Server(port)
  server.setHandler(new ServeCassConfigHandler)
  server.start()

  def stop() {
    server.stop()
  }

  class ServeCassConfigHandler extends AbstractHandler {

    def handle(target: String, baseRequest: Request, request: HttpServletRequest, response: HttpServletResponse) = {

      info(s"Serving config resource: ${target}")

      val plainFilename = new File(new URI(target).getPath).getName // don't ask...
      val confFile = new File(s"${cassConfigDir}/${plainFilename}")

      if (!confFile.exists()){
        throw new FileNotFoundException(s"Couldn't file config file: ${cassConfigDir}/${plainFilename}. Please make sure it exists.")
      }

      val fileContent = Source.fromFile(confFile).getLines()

      val substitutedContent = fileContent.map {
        _.replaceAllLiterally("${seedNodes}", seedNodes
          .mkString(","))
      }.mkString("\n")

      response.setContentType("application/octet-stream;charset=utf-8")
      response.setHeader("Content-Disposition", s"""attachment; filename="${plainFilename}" """)
      response.setHeader("Content-Transfer-Encoding", "binary")
      response.setHeader("Content-Length", substitutedContent.length.toString)

      response.setStatus(HttpServletResponse.SC_OK)
      baseRequest.setHandled(true)
      response.getWriter().println(substitutedContent)
    }

  }

}