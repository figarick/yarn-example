package ru.apapikyan.learn.bigdata.yarn;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TestHttpServer {

	// public static void main(String[] args) throws Exception {
	// HttpServer server = HttpServer.create(new InetSocketAddress(8777), 0);
	// server.createContext("/run-yarn-app", new SimpleAppHandler());
	// server.setExecutor(null); // creates a default executor
	// server.start();
	// }

	public TestHttpServer(int counter) throws IOException {
		HttpServer server = HttpServer.create(new InetSocketAddress(8777 + counter), 0);
		server.createContext("/run-yarn-app" + counter, new SimpleAppHandler());
		server.setExecutor(null); // creates a default executor
		server.start();
	}

	static class SimpleAppHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange t) throws IOException {
			System.out.println(t.getRequestURI());
			System.out.println(t.getRequestHeaders());
			System.out.println(t.getRequestMethod());
			System.out.println(t.getResponseHeaders());

			String response = "<html><body><form>Push the submit button: <input type=\"submit\"></form></body></html>";

			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}

	}
}
