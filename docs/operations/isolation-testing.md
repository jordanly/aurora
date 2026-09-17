# Pi isolation kernel testing

This recipe qualifies the simple argv isolation profile in a disposable ARM64
Debian KVM guest. The qualification host already booted with
`cgroup_disable=memory`; this procedure **does not change host boot settings**.
Pause heavy builds and require at least 2 GiB available before creating the
1 GiB guest. Requirements: Linux ARM64, `/dev/kvm`, existing Docker access,
`curl`, `ssh`, `ssh-keygen`, `tar`, `rg`, and checksum tools. Where Docker
access uses an existing group, run the Docker commands through `sg docker -c`
instead of changing group membership.

## Create a fresh guest

Run from the repository root in Bash. These commands create a new private lab
and do not reuse an existing VM, cgroup delegation, or task root.

```bash
set -euo pipefail
umask 077
REPO=$PWD
mkdir -p "$REPO/.pi-lab"
LAB=$(mktemp -d "$REPO/.pi-lab/isolation-kernel-XXXXXXXX")
VM_NAME="aurora-isolation-$(basename "$LAB")"
mkdir "$LAB/images" "$LAB/seed" "$LAB/build"
cat > "$LAB/build/Dockerfile" <<'DOCKERFILE'
FROM debian:bookworm-slim@sha256:6bd27d44e6c32a66bbd72d7cb2b76a8ae3497ec2e5274a81abd1b37f6013fa1f
RUN apt-get update \
 && apt-get install --no-install-recommends -y \
    cloud-image-utils openssh-client qemu-efi-aarch64 qemu-system-arm qemu-utils \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
DOCKERFILE
docker build -t "$VM_NAME" "$LAB/build"
VM_IMAGE=$(docker image inspect "$VM_NAME" --format '{{.Id}}')
printf '%s\n' "$VM_IMAGE" > "$LAB/container-image.sha256"
docker run --rm --cap-drop=ALL "$VM_IMAGE" \
  sha256sum /usr/share/AAVMF/AAVMF_CODE.fd > "$LAB/firmware.sha256"
docker run --rm --cap-drop=ALL "$VM_IMAGE" \
  dpkg-query -W > "$LAB/container-packages.txt"

CLOUD_URL=https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-arm64.qcow2
CLOUD_SHA512=9eeabc75e74a682fcaff785f6c1f6896b486159397201f51f58298a8f95990f6debb8058b6969ca95cf6823f6e8058dd6f8589ba940a194b2780cd7f95771d69
curl --fail --location --proto '=https' --proto-redir '=https' \
  "$CLOUD_URL" -o "$LAB/images/base.qcow2"
printf '%s  %s\n' "$CLOUD_SHA512" "$LAB/images/base.qcow2" | sha512sum --check
ssh-keygen -q -t ed25519 -N '' -C "$VM_NAME" -f "$LAB/id_ed25519"
cat > "$LAB/seed/user-data" <<CLOUD
#cloud-config
hostname: aurora-isolation
users:
  - name: aurora
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - $(cat "$LAB/id_ed25519.pub")
ssh_pwauth: false
disable_root: true
package_update: false
CLOUD
printf 'instance-id: %s\nlocal-hostname: aurora-isolation\n' "$VM_NAME" > "$LAB/seed/meta-data"
docker run --rm --cap-drop=ALL --user "$(id -u):$(id -g)" \
  -v "$LAB/images:/vm" "$VM_IMAGE" qemu-img create \
  -f qcow2 -F qcow2 -b /vm/base.qcow2 /vm/overlay.qcow2 8G
docker run --rm --cap-drop=ALL --user "$(id -u):$(id -g)" \
  -v "$LAB/seed:/seed" "$VM_IMAGE" cloud-localds \
  /seed/seed.iso /seed/user-data /seed/meta-data
VM_ID=$(docker run -d --name "$VM_NAME" --device=/dev/kvm --cap-drop=ALL \
  --user "$(id -u):$(id -g)" --group-add "$(stat -c %g /dev/kvm)" \
  -p 127.0.0.1:22263:22263 \
  -v "$LAB/images:/vm" -v "$LAB/seed:/seed:ro" "$VM_IMAGE" \
  qemu-system-aarch64 -accel kvm -M virt -cpu host -m 1024M -smp 2 \
  -nographic -bios /usr/share/AAVMF/AAVMF_CODE.fd \
  -drive if=none,file=/vm/overlay.qcow2,format=qcow2,id=disk \
  -device virtio-blk-device,drive=disk \
  -drive if=none,format=raw,file=/seed/seed.iso,readonly=on,id=seed \
  -device virtio-blk-device,drive=seed \
  -netdev user,id=n1,hostfwd=tcp:0.0.0.0:22263-:22 \
  -device virtio-net-device,netdev=n1)
printf '%s\n' "$VM_ID" > "$LAB/container-id"
SSH=(ssh -i "$LAB/id_ed25519" -o UserKnownHostsFile="$LAB/known_hosts" \
  -o StrictHostKeyChecking=accept-new -p 22263 aurora@127.0.0.1)
for n in {1..90}; do
  if "${SSH[@]}" -o ConnectTimeout=2 true; then break; fi
  sleep 2
done
"${SSH[@]}" 'sudo cloud-init status --wait; uname -a' | tee "$LAB/guest.txt"
```

The cloud-image SHA512 above is the exact qualified artifact, checked against
Debian's `SHA512SUMS` at download. The `latest` URL is mutable: a changed image
**must fail checksum verification**. Use an archived copy with that digest to
reproduce this qualification, or explicitly qualify and record a new upstream
image/digest. Apt package versions are recorded, not frozen by the base-image
pin. The qualified container image was
`sha256:bb1fc3bdee9a5f173ea960c2c231d5116c305e65664dca466b092cb7440e56b5`,
and its AAVMF firmware SHA256 was
`5f8ef96257f27e2815270bc54cbf6923bb344cbb5cd72be5b392c2ee4939181a`.

Only loopback SSH is published. No privileged container, Docker socket, host
networking, host boot change, or existing workload directory is needed. First
SSH use trusts the new key on the local VM port; preserve `known_hosts` and use
`StrictHostKeyChecking=yes` for subsequent sessions. If port 22263 is occupied,
choose a fresh port consistently in QEMU, Docker, and SSH.

## Compile and stage the actual test binary

The `agents` staging command does not compile Go tests. Extract the pinned SDK
and explicitly compile this suite. These commands fetch the same SDK pin as
`tools/go-tools.json`; module downloads remain checked by `agent/go.sum`.

```bash
curl --fail --location --proto '=https' --proto-redir '=https' \
  https://go.dev/dl/go1.27.1.linux-arm64.tar.gz -o "$LAB/go.tar.gz"
printf '%s  %s\n' \
  3450b45a3f9ee8568792736a5c5e70a1f2e9b36c35a8f74958c03e51d7d92bec \
  "$LAB/go.tar.gz" | sha256sum --check
mkdir "$LAB/sdk" "$LAB/stage" "$LAB/stage/agent"
tar -xzf "$LAB/go.tar.gz" -C "$LAB/sdk" --no-same-owner --no-same-permissions
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOTOOLCHAIN=local GOENV=off GOWORK=off \
  GOMODCACHE="$LAB/gomodcache" GOCACHE="$LAB/gocache" \
  "$LAB/sdk/go/bin/go" -C "$REPO/agent" test -c \
  -o "$LAB/stage/agent/agent-static.test" .
sha256sum "$LAB/stage/agent/agent-static.test" > "$LAB/test-binary.sha256"
cp -a protocol "$LAB/stage/protocol"
rg --files -0 agent protocol | sort -z | xargs -0 sha256sum > "$LAB/test-sources.sha256"
"${SSH[@]}" 'sudo mkdir -m 700 /root/aurora-kernel /root/isolation-lab'
tar -C "$LAB/stage" -cf - agent protocol | \
  "${SSH[@]}" 'sudo tar -xf - -C /root/aurora-kernel'
"${SSH[@]}" 'sudo sh -eu -c '\''
  mkdir /sys/fs/cgroup/aurora-tests
  chown root:root /sys/fs/cgroup/aurora-tests
  chmod 700 /sys/fs/cgroup/aurora-tests
  test -z "$(cat /sys/fs/cgroup/aurora-tests/cgroup.procs)"
  echo "+cpu +memory +pids" > /sys/fs/cgroup/aurora-tests/cgroup.subtree_control
  cat /sys/fs/cgroup/aurora-tests/cgroup.subtree_control
'\'''
"${SSH[@]}" 'sudo unshare --mount --propagation private sh -eu -c '\''
  cd /root/aurora-kernel/agent
  sha256sum agent-static.test
  timeout 240s env AURORA_ISOLATION_TEST_ROOT=/root/isolation-lab \
    AURORA_ISOLATION_TEST_CGROUP=/sys/fs/cgroup/aurora-tests \
    ./agent-static.test -test.run "^TestIsolationKernelEnforcement$" -test.v
'\''' | tee "$LAB/kernel.log"
```

The working directory matters: tests read `../protocol/native-v1alpha1/fixtures`.
The test creates its trusted rootfs under `/root/isolation-lab`, copies the
static executable into it, and creates separate runtime state there. The
executable and protocol fixtures live outside that artifact root. A skipped
test is not a qualification. Require all five subtests and the suite to PASS.
The test validates `pids.max` configuration; it does **not** perform a fork-bomb
or measure hitting the pids limit.

Before stopping the guest, require no child directories or processes beneath
`/sys/fs/cgroup/aurora-tests`, no surviving test supervisors/workloads, and no
` - tmpfs aurora-` entries in any surviving `/proc/<pid>/mountinfo`. Inspect
process executable, start identity, and immutable attempt spec before cleaning
any failed run. Terminal supervisors can be acknowledged through their own
protocol using the durable import cursor; do not use broad `pkill` or delete
mounted work directories. Preserve logs and overlay for failures. Finally,
verify `docker inspect "$VM_ID"` has the recorded name/image, then stop that
exact ID with `docker stop "$VM_ID"`. Preserve the overlay and receipts until
review is complete; do not remove unrelated containers or artifacts.

## Recorded qualification

On 2026-09-17, guest kernel `6.12.107+deb13-cloud-arm64` completed all five
cases in 26.60 seconds using binary SHA256
`c015918c481915917ee2ea78b865902eaf86d6eb823b1bebd4f17c2e7b2ff2ad`:

| Case | Observed result |
| --- | --- |
| boundary | World-writable rootfs probe still returns EROFS; distinct UID, no capabilities, no-new-privileges, parent signal/proc access denied; bounded /work reaches ENOSPC |
| memory | 64 MiB cgroup limit kills workload with SIGKILL; delegated `oom_kill` increases 4 to 6; cleanup complete |
| cpu | `cpu.max` is `100000 1000000`; `nr_throttled=1`; workload consumes 226575 CPU microseconds over 2320 ms; recovers after daemon/store restart |
| health | Parent observes Ready, releases listener-close gate, then records health-check-failed and complete cleanup |
| escape | A descendant in a new session is killed during cgroup cleanup; task group disappears |

Postcheck found zero task cgroups, test processes (including zombies), and
owned tmpfs mounts across surviving mount namespaces. One terminal supervisor
from an earlier failed qualification was identified by exact executable/start
identity/spec, confirmed SIGKILL/cleanup-complete, and acknowledged naturally.
The five-case run and clean postcheck are evidence for this profile, not for
network isolation, persistent task files across reboot, or graph execution.
See [the isolation contract](native-process-isolation.md).
