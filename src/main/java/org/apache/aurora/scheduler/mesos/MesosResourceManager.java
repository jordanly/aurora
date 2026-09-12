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
package org.apache.aurora.scheduler.mesos;

import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Resource;

import static org.apache.aurora.scheduler.mesos.MesosResourceType.BY_MESOS_NAME;
import static org.apache.aurora.scheduler.mesos.MesosResourceType.fromResource;

/** Resource filtering and conversion at the temporary Mesos adapter boundary. */
public final class MesosResourceManager {
  private MesosResourceManager() {
  }

  /**
   * TODO(maxim): reduce visibility by redirecting callers to #getRevocableOfferResources().
   */
  public static final Predicate<Resource> REVOCABLE =
      r -> !fromResource(r).isRevocable() || r.hasRevocable();

  /**
   * TODO(maxim): reduce visibility by redirecting callers to #getNonRevocableOfferResources().
   */
  public static final Predicate<Resource> NON_REVOCABLE = r -> !r.hasRevocable();

  private static final Function<Resource, ResourceType> MESOS_RESOURCE_TO_TYPE =
      r -> fromResource(r);

  private static final Function<Resource, Double> QUANTIFY_MESOS_RESOURCE =
      r -> MesosResourceType.getMesosResourceConverter(fromResource(r)).quantify(r);

  private static final BinaryOperator<Double> REDUCE_VALUES = (l, r) -> l + r;

  /**
   * TODO(rdelvalle): Remove filters when arbitrary resources are fully supported (AURORA-1328).
   */
  private static final Predicate<Resource> SUPPORTED_RESOURCE =
      r -> BY_MESOS_NAME.containsKey(r.getName());

  /**
   * Gets offer resources matching specified {@link ResourceType}.
   *
   * @param offer Offer to get resources from.
   * @param type {@link ResourceType} to filter resources by.
   * @return Offer resources matching {@link ResourceType}.
   */
  public static Iterable<Resource> getOfferResources(Offer offer, ResourceType type) {
    return Iterables.filter(
        Iterables.filter(offer.getResourcesList(), SUPPORTED_RESOURCE),
        r -> fromResource(r).equals(type));
  }

  /**
   * Gets Mesos-revocable offer resources.
   *
   * @param offer Offer to get resources from.
   * @return Mesos-revocable offer resources.
   */
  public static Iterable<Resource> getRevocableOfferResources(Offer offer) {
    return Iterables.filter(
        offer.getResourcesList(),
        Predicates.and(SUPPORTED_RESOURCE, REVOCABLE));
  }

  /**
   * Gets non-Mesos-revocable offer resources.
   *
   * @param offer Offer to get resources from.
   * @return Non-Mesos-revocable offer resources.
   */
  public static Iterable<Resource> getNonRevocableOfferResources(Offer offer) {
    return Iterables.filter(
        offer.getResourcesList(),
        Predicates.and(SUPPORTED_RESOURCE, NON_REVOCABLE));
  }

  /**
   * Gets offer resources filtered by the provided {@code tierInfo} instance.
   *
   * @param offer Offer to get resources from.
   * @param revocable if {@code true} return only revocable resources,
   *                  if {@code false} return non-revocable.
   * @return Offer resources filtered by {@code tierInfo}.
   */
  public static Iterable<Resource> getOfferResources(Offer offer, boolean revocable) {
    return revocable
        ? getRevocableOfferResources(offer)
        : getNonRevocableOfferResources(offer);
  }

  /**
   * Gets offer resoruces filtered by the {@code tierInfo} and {@code type}.
   *
   * @param offer Offer to get resources from.
   * @param revocable if {@code true} return only revocable resources,
   *                  if {@code false} return non-revocable.
   * @param type Resource type.
   * @return Offer resources filtered by {@code tierInfo} and {@code type}.
   */
  public static Iterable<Resource> getOfferResources(
      Offer offer,
      boolean revocable,
      ResourceType type) {

    return Iterables.filter(getOfferResources(offer, revocable), r -> fromResource(r).equals(type));
  }

  /**
   * Gets the quantity of the Mesos resource specified by {@code type}.
   *
   * @param resources Mesos resources.
   * @param type Type of resource to quantify.
   * @return Aggregate Mesos resource value.
   */
  public static Double quantityOfMesosResource(Iterable<Resource> resources, ResourceType type) {
    return StreamSupport.stream(resources.spliterator(), false)
        .filter(r -> SUPPORTED_RESOURCE.apply(r))
        .filter(r -> fromResource(r).equals(type))
        .map(QUANTIFY_MESOS_RESOURCE)
        .reduce(REDUCE_VALUES)
        .orElse(0.0);
  }

  /**
   * Creates a {@link ResourceBag} from Mesos resources.
   *
   * @param resources Mesos resources to convert.
   * @return A {@link ResourceBag} instance.
   */
  public static ResourceBag bagFromMesosResources(Iterable<Resource> resources) {
    return ResourceManager.bagFromResources(
        Iterables.filter(resources, SUPPORTED_RESOURCE),
        MESOS_RESOURCE_TO_TYPE,
        QUANTIFY_MESOS_RESOURCE);
  }

}
