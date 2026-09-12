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

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.scheduler.execution.OfferTransport;
import org.apache.aurora.scheduler.execution.PreparedTask;
import org.apache.mesos.v1.Protos;

import static java.util.Objects.requireNonNull;

/** Native transport operations, with offer consumption and exception policy owned by the caller. */
public final class MesosOfferTransport implements OfferTransport {
  private final Driver driver;

  @Inject
  public MesosOfferTransport(Driver driver) {
    this.driver = requireNonNull(driver);
  }

  @Override
  public void launch(String offerId, PreparedTask task, double refuseSeconds) {
    Protos.Offer.Operation launch = Protos.Offer.Operation.newBuilder()
        .setType(Protos.Offer.Operation.Type.LAUNCH)
        .setLaunch(Protos.Offer.Operation.Launch.newBuilder()
            .addTaskInfos(((MesosPreparedTask) task).getTaskInfo()))
        .build();
    driver.acceptOffers(Protos.OfferID.newBuilder().setValue(offerId).build(),
        ImmutableList.of(launch), filter(refuseSeconds));
  }

  @Override
  public void decline(String offerId, double refuseSeconds) {
    driver.declineOffer(
        Protos.OfferID.newBuilder().setValue(offerId).build(), filter(refuseSeconds));
  }

  private static Protos.Filters filter(double refuseSeconds) {
    return Protos.Filters.newBuilder().setRefuseSeconds(refuseSeconds).build();
  }
}
