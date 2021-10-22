# Overlay DB Notes

Overlay database interfaces are generally useful. In EtherCattle, we used them
to have an NVME overlay volume with a snapshotted underlay volume, meaning we
could sync from Kafka and launch our servers much more quickly than we could
with a "cold" underlay volume. We also used them so that we could test syncing
a Geth server to verify the integrity of the underlay without altering its
state.

With Cardinal, I imagine using Overlays with a local overlay volume and a
remote underlay (such as DynamoDB or BigTable).

When a node starts up, it would get the streams resumption token from the
underlay, and sync all updates into the overlay. The underlay can then continue
to move forward, and synchronization between overlay and underlay isn't a
concern because the overlay will have all the changes that have occurred since
the resumption token, and will only go back to the underlay for data that has
not changed since the resumption token. The underlay need not even be synced
continuously - it could have periodic processes that come online and sync it up
from the stream, and shutdown for a while.
