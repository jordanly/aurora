/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.execution.go;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;

/** Mutual TLS transport; endpoint hostname verification and redirects stay restrictive. */
final class GoAgentClient implements AgentTransport {
  private final HttpClient client;
  private final Duration timeout;

  GoAgentClient(GoAgentConfig config) throws IOException, GeneralSecurityException {
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (InputStream stream = Files.newInputStream(config.keyStore())) {
      keys.load(stream, config.keyStorePassword().toCharArray());
    }
    KeyStore trust = KeyStore.getInstance("PKCS12");
    try (InputStream stream = Files.newInputStream(config.trustStore())) {
      trust.load(stream, config.trustStorePassword().toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keys, config.keyStorePassword().toCharArray());
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trust);
    SSLContext tls = SSLContext.getInstance("TLSv1.3");
    tls.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    timeout = Duration.ofSeconds(15);
    client = HttpClient.newBuilder().sslContext(tls).connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NEVER).build();
  }

  GoAgentClient(HttpClient client, Duration timeout) {
    this.client = java.util.Objects.requireNonNull(client);
    this.timeout = java.util.Objects.requireNonNull(timeout);
    WireJson.require(!timeout.isNegative() && !timeout.isZero(),
        "Positive exchange timeout required");
  }

  public JsonNode request(GoAgentConfig.Node node, String path, JsonNode body,
                   String epoch, String session) throws IOException, InterruptedException {
    HttpRequest.Builder request = HttpRequest.newBuilder(node.url().resolve(path))
        .timeout(timeout).header("X-Aurora-Epoch", epoch)
        .header("X-Aurora-Session", session);
    if (body == null) {
      request.GET();
    } else {
      byte[] bytes = WireJson.bytes(body);
      WireJson.require(bytes.length <= WireJson.MAX_BYTES, "Command exceeds transport bound");
      request.header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
    }
    var exchange = client.sendAsync(request.build(), response -> new BoundedBodySubscriber());
    try {
      HttpResponse<byte[]> response = exchange.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
      JsonNode result = WireJson.parse(response.body());
      boolean commandRejection = response.statusCode() == 409 && body != null
          && "/v1/deliver".equals(path) && result.path("command").isTextual()
          && result.path("bodySha256").isTextual() && result.path("outcome").isTextual();
      if (response.statusCode() != 200 && !commandRejection) {
        throw new IOException("Agent " + node.name() + " returned " + response.statusCode());
      }
      return result;
    } catch (TimeoutException e) {
      exchange.cancel(true);
      throw new IOException("Agent exchange exceeded " + timeout, e);
    } catch (InterruptedException e) {
      exchange.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      throw new IOException("Agent exchange failed: " + e.getCause().getMessage(), e);
    }
  }

  /** Cancels upstream before forwarding a chunk that would exceed the transport limit. */
  private static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
    private final HttpResponse.BodySubscriber<byte[]> delegate =
        HttpResponse.BodySubscribers.ofByteArray();
    private Flow.Subscription subscription;
    private long received;
    private boolean done;

    @Override
    public CompletionStage<byte[]> getBody() {
      return delegate.getBody();
    }

    @Override
    public void onSubscribe(Flow.Subscription upstream) {
      subscription = upstream;
      delegate.onSubscribe(upstream);
    }

    @Override
    public void onNext(List<ByteBuffer> buffers) {
      if (done) {
        return;
      }
      for (ByteBuffer buffer : buffers) {
        received += buffer.remaining();
        if (received > WireJson.MAX_BYTES) {
          done = true;
          subscription.cancel();
          delegate.onError(new IOException("Agent response exceeds 1 MiB"));
          return;
        }
      }
      delegate.onNext(buffers);
    }

    @Override
    public void onError(Throwable failure) {
      if (!done) {
        done = true;
        delegate.onError(failure);
      }
    }

    @Override
    public void onComplete() {
      if (!done) {
        done = true;
        delegate.onComplete();
      }
    }
  }

  @Override
  public void close() {
    client.shutdownNow();
  }
}
