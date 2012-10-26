package com.tedwon.pilot.http.server;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.nio.DefaultServerIOEventDispatch;
import org.apache.http.impl.nio.reactor.DefaultListeningIOReactor;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.entity.ConsumingNHttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.AsyncNHttpServiceHandler;
import org.apache.http.nio.protocol.EventListener;
import org.apache.http.nio.protocol.NHttpRequestHandler;
import org.apache.http.nio.protocol.NHttpRequestHandlerResolver;
import org.apache.http.nio.protocol.SimpleNHttpRequestHandler;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.ListeningIOReactor;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpProcessor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Basic, yet fully functional and spec compliant, HTTP/1.1 server based on the non-blocking
 * I/O model.
 */
public class AsyncNHttpServer {

    /**
     * SLF4J Logging
     */
    private static Logger logger = LoggerFactory.getLogger(AsyncNHttpServer.class);

    private static int ioreactorno = 10;

    private long counter;
    private long throughput;

    public void run() throws Exception {

        HttpParams params = new BasicHttpParams();
        params
                .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
                .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 16 * 1024)
                .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
                .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpComponents/1.1");

        BasicHttpProcessor httpproc = new BasicHttpProcessor();
        httpproc.addInterceptor(new ResponseDate());
        httpproc.addInterceptor(new ResponseServer());
        httpproc.addInterceptor(new ResponseContent());
        httpproc.addInterceptor(new ResponseConnControl());

        AsyncNHttpServiceHandler handler = new AsyncNHttpServiceHandler(
                httpproc,
                new DefaultHttpResponseFactory(),
                new DefaultConnectionReuseStrategy(),
                params);

        handler.setHandlerResolver(
                new SimpleNHttpRequestHandlerResolver(requestHandler));
        handler.setExpectationVerifier(null);
        handler.setEventListener(new EventLogger());

        IOEventDispatch ioEventDispatch = new DefaultServerIOEventDispatch(handler, params);
        ListeningIOReactor ioReactor = new DefaultListeningIOReactor(ioreactorno, params);

        try {
            ioReactor.listen(new InetSocketAddress(8087));
            ioReactor.execute(ioEventDispatch);
        } catch (InterruptedIOException ex) {
            System.err.println("Interrupted");
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }

        logger.info("Shutdown");
    }

    public AsyncNHttpServer() {

        Timer t = new Timer("HTTP Input Adapter Stat", true);
        t.scheduleAtFixedRate(new TimerTask() {

            long currentInputCount = 0;
            long beforeInputCount = 0;

            public void run() {

                currentInputCount = counter;
                throughput = currentInputCount - beforeInputCount;

                logger.info("{},{}", currentInputCount, throughput);

                beforeInputCount = currentInputCount;

            }

        }, 0L, 1000);
    }

    NHttpRequestHandler requestHandler = new SimpleNHttpRequestHandler() {

        public ConsumingNHttpEntity entityRequest(
                final HttpEntityEnclosingRequest request,
                final HttpContext context) {
            return null;
        }

        @Override
        public void handle(
                final HttpRequest request,
                final HttpResponse response,
                final HttpContext context) throws HttpException, IOException {
            String s = request.getRequestLine().getUri();
            System.out.println(s);

            counter++;

            response.setStatusCode(HttpStatus.SC_OK);
            NStringEntity entity = new NStringEntity("", "UTF-8");
            entity.setContentType("text/html; charset=UTF-8");
            response.setEntity(entity);
        }
    };

    public static void main(String[] args) throws Exception {

        AsyncNHttpServer server = new AsyncNHttpServer();
        server.run();
    }

    static class EventLogger implements EventListener {

        public void connectionOpen(final NHttpConnection conn) {
            logger.info("Connection open: " + conn);
        }

        public void connectionTimeout(final NHttpConnection conn) {
            logger.info("Connection timed out: " + conn);
        }

        public void connectionClosed(final NHttpConnection conn) {
            logger.info("Connection closed: " + conn);
        }

        public void fatalIOException(final IOException ex, final NHttpConnection conn) {
            logger.info("I/O error: " + ex.getMessage());
        }

        public void fatalProtocolException(final HttpException ex, final NHttpConnection conn) {
            logger.info("HTTP error: " + ex.getMessage());
        }

    }


    public class SimpleNHttpRequestHandlerResolver implements NHttpRequestHandlerResolver {

        private final NHttpRequestHandler handler;

        public SimpleNHttpRequestHandlerResolver(final NHttpRequestHandler handler) {
            this.handler = handler;
        }

        public NHttpRequestHandler lookup(final String requestURI) {
            return this.handler;
        }
    }
}