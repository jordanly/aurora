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
package org.apache.aurora.scheduler.resources;

import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.gen.Resource._Fields;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.thrift.TEnum;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.quantity.Data.GB;
import static org.apache.aurora.common.quantity.Data.MB;
import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.DOUBLE;
import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.LONG;
import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.STRING;
import static org.apache.aurora.scheduler.resources.ResourceMapper.PORT_MAPPER;
import static org.apache.aurora.scheduler.resources.ResourceSettings.NOT_REVOCABLE;

/**
 * Describes backend-neutral Aurora resource types and their scheduling traits.
 */
@VisibleForTesting
public enum ResourceType implements TEnum {

  /**
   * CPU resource.
   */
  CPUS(
      _Fields.NUM_CPUS,
      DOUBLE,
      Optional.empty(),
      "CPU",
      "core(s)",
      16,
      false,
          // TODO(wfarner): Figure out why checkstyle wants this indentation.
          () -> CommandLine.legacyGetStaticOptions().resourceSettings.enableRevocableCpus),

  /**
   * RAM resource.
   */
  RAM_MB(
      _Fields.RAM_MB,
      LONG,
      Optional.empty(),
      "RAM",
      "MB",
      Amount.of(24, GB).as(MB),
      false,
          () -> CommandLine.legacyGetStaticOptions().resourceSettings.enableRevocableRam),

  /**
   * DISK resource.
   */
  DISK_MB(
      _Fields.DISK_MB,
      LONG,
      Optional.empty(),
      "disk",
      "MB",
      Amount.of(450, GB).as(MB),
      false,
      NOT_REVOCABLE),

  /**
   * Port resource.
   */
  PORTS(
      _Fields.NAMED_PORT,
      STRING,
      Optional.of(PORT_MAPPER),
      "ports",
      "count",
      1000,
      true,
      NOT_REVOCABLE),

  /**
   * GPU resource.
   */
  GPUS(
      _Fields.NUM_GPUS,
      LONG,
      Optional.empty(),
      "GPU",
      "core(s)",
      4,
      false,
      NOT_REVOCABLE);

  public static void initializeEmptyCliArgsForTest() {
    CommandLine.initializeForTest();
  }

  /**
   * Correspondent thrift {@link org.apache.aurora.gen.Resource} enum value.
   */
  private final _Fields value;

  /**
   * Type converter for resource values.
   */
  private final AuroraResourceConverter<?> auroraResourceConverter;

  /**
   * Optional resource mapper to use.
   */
  private final Optional<ResourceMapper<?>> mapper;

  /**
   * Aurora resource name.
   */
  private final String auroraName;

  /**
   * Aurora resource unit.
   */
  private final String auroraUnit;

  /**
   * Scaling range for comparing scheduling vetoes.
   */
  private final int scalingRange;

  /**
   * Indicates if multiple resource types are allowed in a task.
   */
  private final boolean multipleAllowed;

  /**
   * Indicates if a resource can be revocable.
   */
  private final Supplier<Boolean> revocable;

  private static ImmutableMap<Integer, ResourceType> byField =
      Maps.uniqueIndex(EnumSet.allOf(ResourceType.class),  ResourceType::getValue);

  /**
   * Describes a Resource type.
   *
   * @param value Correspondent {@link _Fields} value.
   * @param auroraResourceConverter See {@link #getAuroraResourceConverter()} for more details.
   * @param mapper See {@link #getMapper()} for more details.
   * @param auroraName See {@link #getAuroraName()} for more details.
   * @param auroraUnit See {@link #getAuroraUnit()} for more details.
   * @param scalingRange See {@link #getScalingRange()} for more details.
   * @param isMultipleAllowed See {@link #isMultipleAllowed()} for more details.
   * @param isRevocable See {@link #isRevocable()} for more details.
   */
  ResourceType(
      _Fields value,
      AuroraResourceConverter<?> auroraResourceConverter,
      Optional<ResourceMapper<?>> mapper,
      String auroraName,
      String auroraUnit,
      int scalingRange,
      boolean isMultipleAllowed,
      Supplier<Boolean> isRevocable) {

    this.value = value;
    this.auroraResourceConverter = requireNonNull(auroraResourceConverter);
    this.mapper = requireNonNull(mapper);
    this.auroraName = requireNonNull(auroraName);
    this.auroraUnit = requireNonNull(auroraUnit);
    this.scalingRange = scalingRange;
    this.multipleAllowed = isMultipleAllowed;
    this.revocable = isRevocable;
  }

  /**
   * Get unique ID value.
   *
   * @return Enum ID.
   */
  @Override
  public int getValue() {
    return value.getThriftFieldId();
  }

  /**
   * Gets {@link AuroraResourceConverter} to convert resource values.
   *
   * @return {@link AuroraResourceConverter} instance.
   */
  public AuroraResourceConverter<?> getAuroraResourceConverter() {
    return auroraResourceConverter;
  }

  /**
   * Gets optional resource mapper. See {@link ResourceMapper} for more details.
   *
   * @return Optional ResourceMapper.
   */
  public Optional<ResourceMapper<?>> getMapper() {
    return mapper;
  }

  /**
   * Gets resource name for internal Aurora representation (e.g. in the UI).
   *
   * @return Aurora resource name.
   */
  public String getAuroraName() {
    return auroraName;
  }

  /**
   * Gets resource unit for internal Aurora representation.
   *
   * @return Aurora resource unit.
   */
  public String getAuroraUnit() {
    return auroraUnit;
  }

  /**
   * Gets "stats-friendly" unit for using in metrics.
   *
   * @return
   */
  public String getAuroraStatUnit() {
    return auroraUnit.replaceAll("\\(|\\)", "");
  }

  /**
   * Scaling range to use for comparison of scheduling vetoes.
   * <p>
   * This has no real bearing besides trying to determine if a veto along one resource vector
   * is a 'stronger' veto than that of another vector. The value represents the typical slave
   * machine resources.
   *
   * @return Resource scaling range.
   */
  public int getScalingRange() {
    return scalingRange;
  }

  /**
   * Returns a flag indicating if multiple resource of the same type are allowed in a given task.
   *
   * @return True if multiple resources of the same type are allowed, false otherwise.
   */
  public boolean isMultipleAllowed() {
    return multipleAllowed;
  }

  /**
   * Returns a flag indicating if a resource can be revocable.
   *
   * @return True if a resource can be revocable, false otherwise.
   */
  public boolean isRevocable() {
    return revocable.get();
  }

  /**
   * Returns a {@link ResourceType} for the given ID.
   *
   * @param value ID value to search by. See {@link #getValue()}.
   * @return {@link ResourceType}.
   */
  public static ResourceType fromIdValue(int value) {
    ResourceType resourceType = byField.get(value);
    if (resourceType == null) {
      throw new NullPointerException("Unmapped value: " + value);
    }
    return resourceType;
  }

  /**
   * Returns a {@link ResourceType} for the given resource.
   *
   * @param resource {@link IResource} to search by.
   * @return {@link ResourceType}.
   */
  public static ResourceType fromResource(IResource resource) {
    ResourceType resourceType = byField.get((int) resource.getSetField().getThriftFieldId());
    if (resourceType == null) {
      throw new NullPointerException("Unknown resource: " + resource);
    }
    return resourceType;
  }

}
