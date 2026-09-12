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
package org.apache.aurora.scheduler.offers;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;

public class OfferManagerImpl implements OfferManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(OfferManagerImpl.class);

  @VisibleForTesting
  static final String OFFER_ACCEPT_RACES = "offer_accept_races";
  @VisibleForTesting
  static final String OUTSTANDING_OFFERS = "outstanding_offers";
  @VisibleForTesting
  static final String STATICALLY_BANNED_OFFERS = "statically_banned_offers_size";
  @VisibleForTesting
  static final String STATICALLY_BANNED_OFFERS_HIT_RATE = "statically_banned_offers_hit_rate";
  @VisibleForTesting
  static final String OFFER_CANCEL_FAILURES = "offer_cancel_failures";
  @VisibleForTesting
  static final String GLOBALLY_BANNED_OFFERS = "globally_banned_offers_size";
  @VisibleForTesting
  static final String VETO_EVALUATED_OFFERS = "veto_evaluated_offers";

  private final HostOffers hostOffers;
  private final AtomicLong offerRaces;
  private final AtomicLong offerCancelFailures;

  private final OfferTransport transport;
  private final OfferSettings offerSettings;
  private final Deferment offerDecline;

  @Inject
  @VisibleForTesting
  public OfferManagerImpl(
      OfferTransport transport,
      OfferSettings offerSettings,
      StatsProvider statsProvider,
      Deferment offerDecline,
      SchedulingFilter schedulingFilter) {

    this.transport = requireNonNull(transport);
    this.offerSettings = requireNonNull(offerSettings);
    this.hostOffers = new HostOffers(statsProvider, offerSettings, schedulingFilter);
    this.offerRaces = statsProvider.makeCounter(OFFER_ACCEPT_RACES);
    this.offerCancelFailures = statsProvider.makeCounter(OFFER_CANCEL_FAILURES);
    this.offerDecline = requireNonNull(offerDecline);
  }

  @Override
  public void add(HostOffer offer) {
    Optional<HostOffer> sameAgent = hostOffers.addAndPreventAgentCollision(offer);
    if (sameAgent.isPresent()) {
      // We have an existing offer for the same agent.  We choose to return both offers so that
      // they may be combined into a single offer.
      LOG.info("Returning offers for " + offer.getAgentId()
          + " for compaction.");
      decline(offer.getOfferId());
      decline(sameAgent.get().getOfferId());
    } else {
      offerDecline.defer(() -> removeAndDecline(offer.getOfferId()));
    }
  }

  private void removeAndDecline(String id) {
    if (removeFromHostOffers(id)) {
      decline(id);
    }
  }

  private void decline(String id) {
    LOG.debug("Declining offer {}", id);
    transport.decline(id, getRefuseSeconds());
  }

  private double getRefuseSeconds() {
    return offerSettings.getFilterDuration().as(Time.SECONDS);
  }

  @Override
  public boolean cancel(final String offerId) {
    boolean success = removeFromHostOffers(offerId);
    if (!success) {
      // This will happen rarely when we race to process this rescind against accepting the offer
      // to launch a task.
      // If it happens frequently, we are likely processing rescinds before the offer itself.
      LOG.warn("Failed to cancel offer: {}.", offerId);
      this.offerCancelFailures.incrementAndGet();
    }
    return success;
  }

  private boolean removeFromHostOffers(final String offerId) {
    requireNonNull(offerId);

    // The small risk of inconsistency is acceptable here - if we have an accept/remove race
    // on an offer, the master will mark the task as LOST and it will be retried.
    return hostOffers.remove(offerId);
  }

  @Override
  public void ban(String offerId) {
    hostOffers.addGlobalBan(offerId);
  }

  /**
   * Updates the preference of a host's offers.
   *
   * @param change Host change notification.
   */
  @Subscribe
  public void hostAttributesChanged(PubsubEvent.HostAttributesChanged change) {
    hostOffers.updateHostAttributes(change.getAttributes());
  }

  @Override
  public Optional<HostOffer> get(String agentId) {
    return hostOffers.get(agentId);
  }

  @Override
  public Iterable<HostOffer> getAll() {
    return hostOffers.getOffers();
  }

  @Override
  public Optional<HostOffer> getMatching(String agentId, ResourceRequest resourceRequest) {
    return hostOffers.getMatching(agentId, resourceRequest);
  }

  @Override
  public Iterable<HostOffer> getAllMatching(TaskGroupKey groupKey,
                                            ResourceRequest resourceRequest) {

    return hostOffers.getAllMatching(groupKey, resourceRequest);
  }

  /**
   * Notifies the queue that the driver is disconnected, and all the stored offers are now
   * invalid.
   * <p>
   * The queue takes this as a signal to flush its queue.
   *
   * @param event Disconnected event.
   */
  @Subscribe
  public void driverDisconnected(PubsubEvent.DriverDisconnected event) {
    LOG.info("Clearing stale offers since the driver is disconnected.");
    hostOffers.clear();
  }

  @Timed("offer_manager_launch_task")
  @Override
  public void launchTask(String offerId, PreparedTask task) throws LaunchException {
    // Guard against an offer being removed after we grabbed it from the iterator.
    // If that happens, the offer will not exist in hostOffers, and we can immediately
    // send it back to LOST for quick reschedule.
    // Removing while iterating counts on the use of a weakly-consistent iterator being used,
    // which is a feature of ConcurrentSkipListSet.
    if (hostOffers.remove(offerId)) {
      try {
        transport.launch(offerId, task, getRefuseSeconds());
      } catch (IllegalStateException e) {
        // TODO(William Farner): Catch only the checked exception produced by Driver
        // once it changes from throwing IllegalStateException when the driver is not yet
        // registered.
        throw new LaunchException("Failed to launch task.", e);
      }
    } else {
      offerRaces.incrementAndGet();
      throw new LaunchException("Offer no longer exists in offer queue, likely data race.");
    }
  }

  /**
   * Get all static bans.
   */
  @VisibleForTesting
  Set<Pair<String, TaskGroupKey>> getStaticBans() {
    return hostOffers.getStaticBans();
  }

  /**
   * Exclude an offer that results in a static mismatch from further attempts to match against all
   * tasks from the same group.
   */
  @VisibleForTesting
  void banForTaskGroup(String offerId, TaskGroupKey groupKey) {
    hostOffers.addStaticGroupBan(offerId, groupKey);
  }

  /**
   * Used for testing to ensure that the underlying cache's `size` method returns an accurate
   * value by not including evicted entries.
   */
  @VisibleForTesting
  void cleanupStaticBans() {
    hostOffers.cleanUpStaticallyBannedOffers();
  }
}
