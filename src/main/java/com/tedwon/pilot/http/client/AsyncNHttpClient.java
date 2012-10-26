package com.tedwon.pilot.http.client;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultClientIOEventDispatch;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.NHttpClientHandler;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.entity.BufferingNHttpEntity;
import org.apache.http.nio.entity.ConsumingNHttpEntity;
import org.apache.http.nio.protocol.AsyncNHttpClientHandler;
import org.apache.http.nio.protocol.EventListener;
import org.apache.http.nio.protocol.NHttpRequestExecutionHandler;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.SessionRequest;
import org.apache.http.nio.reactor.SessionRequestCallback;
import org.apache.http.nio.util.HeapByteBufferAllocator;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpProcessor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Example of a very simple asynchronous connection manager that maintains a pool of persistent
 * connections to one target host.
 */
public class AsyncNHttpClient implements Runnable {

    /**
     * SLF4J Logging
     */
    private static Logger logger = LoggerFactory.getLogger(AsyncNHttpClient.class);

    private static int ioreactorno = 2;

    public void run() {

        try {
            HttpParams params = new BasicHttpParams();
            params
                    .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
                    .setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000)
                    .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
                    .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
                    .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                    .setParameter(CoreProtocolPNames.USER_AGENT, "HttpComponents/1.1");

            BasicHttpProcessor httpproc = new BasicHttpProcessor();
            httpproc.addInterceptor(new RequestContent());
            httpproc.addInterceptor(new RequestTargetHost());
            httpproc.addInterceptor(new RequestConnControl());
            httpproc.addInterceptor(new RequestUserAgent());
            httpproc.addInterceptor(new RequestExpectContinue());

            // Set up protocol handler
            AsyncNHttpClientHandler protocolHandler = new AsyncNHttpClientHandler(
                    httpproc,
                    requestExecutionHandler,
                    new DefaultConnectionReuseStrategy(),
                    params);

            protocolHandler.setEventListener(new EventLogger());

            // Limit the total maximum of concurrent connections to 5
            int maxTotalConnections = 5;

            // Use the connection manager to maintain a pool of connections to localhost:8080
            final AsyncConnectionManager connMgr = new AsyncConnectionManager(
                    new HttpHost("localhost", 8087),
                    maxTotalConnections,
                    protocolHandler,
                    params);

            // Start the I/O reactor in a separate thread
            Thread t = new Thread(new Runnable() {

                public void run() {
                    try {
                        connMgr.execute();
                    } catch (InterruptedIOException ex) {
                        System.err.println("Interrupted");
                    } catch (IOException e) {
                        System.err.println("I/O error: " + e.getMessage());
                    }
                    System.out.println("I/O reactor terminated");
                }

            });
            t.start();

            long counter = 0;

            // Submit 50 requests using maximum 5 concurrent connections
            Queue<RequestHandle> queue = new LinkedList<RequestHandle>();
            for (long i = 0; i < 2; i++) {
                AsyncConnectionRequest connRequest = connMgr.requestConnection();
                connRequest.waitFor();
                NHttpClientConnection conn = connRequest.getConnection();
                if (conn == null) {
                    System.err.println("Failed to obtain connection");
                    break;
                }

                HttpContext context = conn.getContext();

                BasicHttpRequest httpget = new BasicHttpRequest("GET", "/?stream=Event&STRD_DT=20101119", HttpVersion.HTTP_1_1);
                RequestHandle handle = new RequestHandle(connMgr, conn);

                context.setAttribute("request", httpget);
                context.setAttribute("request-handle", handle);

                queue.add(handle);
                conn.requestOutput();

                counter++;
                logger.info("{}", counter);
            }

            // Wait until all requests have been completed
            while (!queue.isEmpty()) {
                RequestHandle handle = queue.remove();
                handle.waitFor();
            }

            // Give the I/O reactor 10 sec to shut down
            connMgr.shutdown(10000);
            System.out.println("Done");

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    NHttpRequestExecutionHandler requestExecutionHandler = new NHttpRequestExecutionHandler() {

        public void initalizeContext(final HttpContext context, final Object attachment) {
            context.setAttribute("LIST", attachment);
            context.setAttribute("REQ-COUNT", Integer.valueOf(0));
            context.setAttribute("RES-COUNT", Integer.valueOf(0));
        }

        public void finalizeContext(final HttpContext context) {
        }

        public HttpRequest submitRequest(final HttpContext context) {
            HttpRequest request = (HttpRequest) context.removeAttribute("request");
            return request;
        }

        public ConsumingNHttpEntity responseEntity(
                final HttpResponse response,
                final HttpContext context) throws IOException {
            return new BufferingNHttpEntity(response.getEntity(),
                    new HeapByteBufferAllocator());
        }

        public void handleResponse(final HttpResponse response, final HttpContext context) {
//                NHttpConnection conn = (NHttpConnection) context.getAttribute(
//                        ExecutionContext.HTTP_CONNECTION);

//            HttpEntity entity = response.getEntity();
//            try {
//                String content = EntityUtils.toString(entity);
//
//                System.out.println("--------------");
//                System.out.println(response.getStatusLine());
//                System.out.println("--------------");
//                System.out.println("Document length: " + content.length());
//                System.out.println("--------------");
//            } catch (IOException ex) {
//                System.err.println("I/O error: " + ex.getMessage());
//            }

            RequestHandle handle = (RequestHandle) context.removeAttribute("request-handle");
            if (handle != null) {
                handle.completed();
            }
        }

    };

    public static void main(String[] args) {

        AsyncNHttpClient runnable = new AsyncNHttpClient();
        Thread reactorThread = new Thread(runnable);
        reactorThread.setDaemon(false);
        reactorThread.start();
    }

    static class AsyncConnectionRequest {

        private volatile boolean completed;
        private volatile NHttpClientConnection conn;

        public AsyncConnectionRequest() {
            super();
        }

        public boolean isCompleted() {
            return this.completed;
        }

        public void setConnection(NHttpClientConnection conn) {
            if (this.completed) {
                return;
            }
            this.completed = true;
            synchronized (this) {
                this.conn = conn;
                notifyAll();
            }
        }

        public NHttpClientConnection getConnection() {
            return this.conn;
        }

        public void cancel() {
            if (this.completed) {
                return;
            }
            this.completed = true;
            synchronized (this) {
                notifyAll();
            }
        }

        public void waitFor() throws InterruptedException {
            if (this.completed) {
                return;
            }
            synchronized (this) {
                while (!this.completed) {
                    wait();
                }
            }
        }

    }

    static class AsyncConnectionManager {

        private final HttpHost target;
        private final int maxConnections;
        private final NHttpClientHandler handler;
        private final HttpParams params;
        private final ConnectingIOReactor ioreactor;
        private final Object lock;
        private final Set<NHttpClientConnection> allConns;
        private final Queue<NHttpClientConnection> availableConns;
        private final Queue<AsyncConnectionRequest> pendingRequests;

        private volatile boolean shutdown;

        public AsyncConnectionManager(
                HttpHost target,
                int maxConnections,
                NHttpClientHandler handler,
                HttpParams params) throws IOReactorException {
            super();
            this.target = target;
            this.maxConnections = maxConnections;
            this.handler = handler;
            this.params = params;
            this.lock = new Object();
            this.allConns = new HashSet<NHttpClientConnection>();
            this.availableConns = new LinkedList<NHttpClientConnection>();
            this.pendingRequests = new LinkedList<AsyncConnectionRequest>();
            this.ioreactor = new DefaultConnectingIOReactor(ioreactorno, params);
        }

        public void execute() throws IOException {
            IOEventDispatch dispatch = new DefaultClientIOEventDispatch(
                    new ManagedClientHandler(this.handler, this), this.params);
            this.ioreactor.execute(dispatch);
        }

        public void shutdown(long waitMs) throws IOException {
            synchronized (this.lock) {
                if (!this.shutdown) {
                    this.shutdown = true;
                    while (!this.pendingRequests.isEmpty()) {
                        AsyncConnectionRequest request = this.pendingRequests.remove();
                        request.cancel();
                    }
                    this.availableConns.clear();
                    this.allConns.clear();
                }
            }
            this.ioreactor.shutdown(waitMs);
        }

        void addConnection(NHttpClientConnection conn) {
            if (conn == null) {
                return;
            }
            if (this.shutdown) {
                return;
            }
            synchronized (this.lock) {
                this.allConns.add(conn);
            }
        }

        void removeConnection(NHttpClientConnection conn) {
            if (conn == null) {
                return;
            }
            if (this.shutdown) {
                return;
            }
            synchronized (this.lock) {
                if (this.allConns.remove(conn)) {
                    this.availableConns.remove(conn);
                }
                processRequests();
            }
        }

        public AsyncConnectionRequest requestConnection() {
            if (this.shutdown) {
                throw new IllegalStateException("Connection manager has been shut down");
            }
            AsyncConnectionRequest request = new AsyncConnectionRequest();
            synchronized (this.lock) {
                while (!this.availableConns.isEmpty()) {
                    NHttpClientConnection conn = this.availableConns.remove();
                    if (conn.isOpen()) {
                        System.out.println("Re-using persistent connection");
                        request.setConnection(conn);
                        break;
                    } else {
                        this.allConns.remove(conn);
                    }
                }
                if (!request.isCompleted()) {
                    this.pendingRequests.add(request);
                    processRequests();
                }
            }
            return request;
        }

        public void releaseConnection(NHttpClientConnection conn) {
            if (conn == null) {
                return;
            }
            if (this.shutdown) {
                return;
            }
            synchronized (this.lock) {
                if (this.allConns.contains(conn)) {
                    if (conn.isOpen()) {
                        conn.setSocketTimeout(0);
                        AsyncConnectionRequest request = this.pendingRequests.poll();
                        if (request != null) {
                            System.out.println("Re-using persistent connection");
                            request.setConnection(conn);
                        } else {
                            this.availableConns.add(conn);
                        }
                    } else {
                        this.allConns.remove(conn);
                        processRequests();
                    }
                }
            }
        }

        private void processRequests() {
            while (this.allConns.size() < this.maxConnections) {
                AsyncConnectionRequest request = this.pendingRequests.poll();
                if (request == null) {
                    break;
                }
                InetSocketAddress address = new InetSocketAddress(
                        this.target.getHostName(),
                        this.target.getPort());
                ConnRequestCallback callback = new ConnRequestCallback(request);
                System.out.println("Opening new connection");
                this.ioreactor.connect(address, null, request, callback);
            }
        }

    }

    static class ManagedClientHandler implements NHttpClientHandler {

        private final NHttpClientHandler handler;
        private final AsyncConnectionManager connMgr;

        public ManagedClientHandler(NHttpClientHandler handler, AsyncConnectionManager connMgr) {
            super();
            this.handler = handler;
            this.connMgr = connMgr;
        }

        public void connected(NHttpClientConnection conn, Object attachment) {
            AsyncConnectionRequest request = (AsyncConnectionRequest) attachment;
            this.handler.connected(conn, attachment);
            this.connMgr.addConnection(conn);
            request.setConnection(conn);
        }

        public void closed(NHttpClientConnection conn) {
            this.connMgr.removeConnection(conn);
            this.handler.closed(conn);
        }

        public void requestReady(NHttpClientConnection conn) {
            this.handler.requestReady(conn);
        }

        public void outputReady(NHttpClientConnection conn, ContentEncoder encoder) {
            this.handler.outputReady(conn, encoder);
        }

        public void responseReceived(NHttpClientConnection conn) {
            this.handler.responseReceived(conn);
        }

        public void inputReady(NHttpClientConnection conn, ContentDecoder decoder) {
            this.handler.inputReady(conn, decoder);
        }

        public void exception(NHttpClientConnection conn, HttpException ex) {
            this.handler.exception(conn, ex);
        }

        public void exception(NHttpClientConnection conn, IOException ex) {
            this.handler.exception(conn, ex);
        }

        public void timeout(NHttpClientConnection conn) {
            this.handler.timeout(conn);
        }

    }

    static class RequestHandle {

        private final AsyncConnectionManager connMgr;
        private final NHttpClientConnection conn;

        private volatile boolean completed;

        public RequestHandle(AsyncConnectionManager connMgr, NHttpClientConnection conn) {
            super();
            this.connMgr = connMgr;
            this.conn = conn;
        }

        public boolean isCompleted() {
            return this.completed;
        }

        public void completed() {
            if (this.completed) {
                return;
            }
            this.completed = true;
            this.connMgr.releaseConnection(this.conn);
            synchronized (this) {
                notifyAll();
            }
        }

        public void cancel() {
            if (this.completed) {
                return;
            }
            this.completed = true;
            synchronized (this) {
                notifyAll();
            }
        }

        public void waitFor() throws InterruptedException {
            if (this.completed) {
                return;
            }
            synchronized (this) {
                while (!this.completed) {
                    wait();
                }
            }
        }

    }

    static class ConnRequestCallback implements SessionRequestCallback {

        private final AsyncConnectionRequest request;

        ConnRequestCallback(AsyncConnectionRequest request) {
            super();
            this.request = request;
        }

        public void completed(SessionRequest request) {
            System.out.println(request.getRemoteAddress() + " - request successful");
        }

        public void cancelled(SessionRequest request) {
            System.out.println(request.getRemoteAddress() + " - request cancelled");
            this.request.cancel();
        }

        public void failed(SessionRequest request) {
            System.err.println(request.getRemoteAddress() + " - request failed");
            IOException ex = request.getException();
            if (ex != null) {
                ex.printStackTrace();
            }
            this.request.cancel();
        }

        public void timeout(SessionRequest request) {
            System.out.println(request.getRemoteAddress() + " - request timed out");
            this.request.cancel();
        }

    }

    static class EventLogger implements EventListener {

        public void connectionOpen(final NHttpConnection conn) {
            System.out.println("Connection open: " + conn);
        }

        public void connectionTimeout(final NHttpConnection conn) {
            System.out.println("Connection timed out: " + conn);
        }

        public void connectionClosed(final NHttpConnection conn) {
            System.out.println("Connection closed: " + conn);
        }

        public void fatalIOException(final IOException ex, final NHttpConnection conn) {
            System.err.println("I/O error: " + ex.getMessage());
        }

        public void fatalProtocolException(final HttpException ex, final NHttpConnection conn) {
            System.err.println("HTTP error: " + ex.getMessage());
        }
    }
}