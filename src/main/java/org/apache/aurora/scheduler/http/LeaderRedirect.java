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
package org.apache.aurora.scheduler.http;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor.MonitorException;
import org.apache.aurora.scheduler.discovery.ServiceInstance;
import org.apache.aurora.scheduler.discovery.ServiceInstance.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Redirect logic for finding the leading scheduler in the event that this process is not the
 * leader.
 */
class LeaderRedirect implements Closeable {

  enum LeaderStatus {
    /**
     * This instance is currently the leading scheduler.
     */
    LEADING,

    /**
     * There is not currently an elected leading scheduler.
     */
    NO_LEADER,

    /**
     * This instance is not currently the leading scheduler.
     */
    NOT_LEADING,
  }

  private static final Logger LOG = LoggerFactory.getLogger(LeaderRedirect.class);

  private final HttpService httpService;
  private final ServiceGroupMonitor serviceGroupMonitor;

  @Inject
  LeaderRedirect(HttpService httpService, ServiceGroupMonitor serviceGroupMonitor) {
    this.httpService = requireNonNull(httpService);
    this.serviceGroupMonitor = requireNonNull(serviceGroupMonitor);
  }

  /**
   * Initiates the monitor that will watch the scheduler host set.
   *
   * @throws MonitorException If monitoring failed to initialize.
   */
  public void monitor() throws MonitorException {
    serviceGroupMonitor.start();
  }

  @Override
  public void close() throws IOException {
    serviceGroupMonitor.close();
  }

  record LeaderObservation(LeaderStatus status, Optional<HostAndPort> redirect) { }

  LeaderObservation observeLeader() {
    Optional<HostAndPort> leader = getLeader().map(scheduler -> {
      Endpoint endpoint = scheduler.getServiceEndpoint();
      return HostAndPort.fromParts(endpoint.getHost(), endpoint.getPort());
    });
    if (leader.isEmpty()) {
      return new LeaderObservation(LeaderStatus.NO_LEADER, Optional.empty());
    }
    if (leader.equals(getLocalHttp())) {
      return new LeaderObservation(LeaderStatus.LEADING, Optional.empty());
    }
    return new LeaderObservation(LeaderStatus.NOT_LEADING, leader);
  }

  private Optional<HostAndPort> getLocalHttp() {
    HostAndPort localHttp = httpService.getAddress();
    return (localHttp == null) ? Optional.empty()
        : Optional.of(HostAndPort.fromParts(localHttp.getHost(), localHttp.getPort()));
  }

  /**
   * Gets the optional HTTP endpoint that should be redirected to in the event that this
   * scheduler is not the leader.
   *
   * @return Optional redirect target.
   */
  @VisibleForTesting
  Optional<HostAndPort> getRedirect() {
    return observeLeader().redirect();
  }

  /**
   * Gets the current status of the elected leader.
   *
   * @return a {@code LeaderStatus} indicating whether there is an elected leader (and if so, if
   * this instance is the leader).
   */
  LeaderStatus getLeaderStatus() {
    return observeLeader().status();
  }

  /**
   * Gets the optional redirect URI target in the event that this process is not the leading
   * scheduler.
   *
   * @param req HTTP request.
   * @return An optional redirect destination to route the request to the leading scheduler.
   */
  Optional<String> getRedirectTarget(HttpServletRequest req) {
    return getRedirectTarget(req, observeLeader());
  }

  Optional<String> getRedirectTarget(HttpServletRequest req, LeaderObservation observation) {
    Optional<HostAndPort> redirectTarget = observation.redirect();
    if (redirectTarget.isPresent()) {
      HostAndPort target = redirectTarget.get();
      StringBuilder redirect = new StringBuilder()
          .append(req.getScheme())
          .append("://")
          .append(target)
          .append(
              // If Jetty rewrote the path, we want to be sure to redirect to the original path
              // rather than the rewritten path to be sure it's a route the UI code recognizes.
              Optional.ofNullable(
                  req.getAttribute(JettyServerModule.ORIGINAL_PATH_ATTRIBUTE_NAME))
                  .orElse(req.getRequestURI()));

      String queryString = req.getQueryString();
      if (queryString != null) {
        redirect.append('?').append(queryString);
      }

      return Optional.of(redirect.toString());
    } else {
      return Optional.empty();
    }
  }

  private Optional<ServiceInstance> getLeader() {
    ImmutableSet<ServiceInstance> hostSet = serviceGroupMonitor.get();
    switch (hostSet.size()) {
      case 0:
        LOG.warn("No serviceGroupMonitor in host set, will not redirect despite not being leader.");
        return Optional.empty();
      case 1:
        LOG.debug("Found leader scheduler at {}", hostSet);
        return Optional.of(Iterables.getOnlyElement(hostSet));
      default:
        LOG.error("Multiple serviceGroupMonitor detected, will not redirect: {}", hostSet);
        return Optional.empty();
    }
  }
}
