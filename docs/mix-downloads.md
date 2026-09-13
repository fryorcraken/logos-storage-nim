# Direct and Mix downloads

## Choosing how a download contacts providers

`mixEnabled` enables the node's Mix services. A download independently selects the connection type used for its manifest and blocks. The choice is represented by `DownloadTransport`, defined in `storage/downloadtransport.nim`:

```nim
type DownloadTransport* {.pure.} = enum
  Direct
  Mix
```

Direct is the default, including on a node with Mix enabled. The network download endpoints accept `?transport=direct` or `?transport=mix`:

- `POST /api/storage/v1/data/{cid}/network` starts a background download.
- `GET /api/storage/v1/data/{cid}/network/stream` streams the dataset.
- `GET /api/storage/v1/data/{cid}/network/manifest` fetches only the manifest.

An unrecognized value produces HTTP 400. A Mix request fails if Mix has not been attached; the request does not fall back to direct dialing. Existing internal callers that omit the parameter select Direct. This increment does not add a C-library download parameter or change the existing DHT proxy selection.

## From the API to a download

The REST handler parses the parameter before fetching the manifest. The background endpoint passes the same value to both `StorageNodeRef.fetchManifest` and `StorageNodeRef.startBackgroundDownload`. Their signatures are:

```nim
proc fetchManifest*(
    self: StorageNodeRef,
    cid: Cid,
    transport: DownloadTransport = DownloadTransport.Direct,
): Future[?!Manifest] {.async: (raises: [CancelledError]).}

proc startBackgroundDownload*(
    self: StorageNodeRef,
    md: ManifestDescriptor,
    selectionPolicy: SelectionPolicy = spSequential,
    transport: DownloadTransport = DownloadTransport.Direct,
): Future[?!uint64] {.async: (raises: [CancelledError]).}
```

`startBackgroundDownload` reuses an existing background download only when both the tree CID and transport match. Otherwise, the download manager allocates another download ID. The manager still owns all downloads; there is no separate manager or ID namespace for Mix.

The engine places the selected transport in `DownloadDesc`, and `DownloadContext.new` copies the value to the long-lived download context. The worker uses that value whenever it selects peers, requests presence, sends block requests, or drops a failed peer. Consequently, changing the node's available services does not reinterpret an existing download as a different transport.

## Keeping peer connections separate

`BlockExcNetwork.new` constructs the ordinary network adapter and a second adapter stored in `mixNetwork`. Only the ordinary adapter is mounted on the Switch. Its protocol handler dispatches an incoming `TransportStream` to the Mix adapter's peer read loop; ordinary connections stay with the direct adapter. Both adapters use the same BlockExchange codec and message implementation.

Each adapter owns its own `Table[PeerId, NetworkPeer]`. This is necessary because `NetworkPeer` caches the connection used for outgoing messages. If the table were shared, a direct download and a Mix download of the same provider could reuse the wrong connection.

The engine selects the adapter with:

```nim
func networkFor*(
    self: BlockExcNetwork, transport: DownloadTransport
): BlockExcNetwork =
  if transport == DownloadTransport.Mix: self.mixNetwork else: self
```

The engine likewise keeps separate direct and Mix peer-context stores and in-flight request trackers. Closing a Mix session removes only the Mix peer context and its tracker entries. The direct connection and its context remain available. The download's swarm still uses peer IDs internally: the entire swarm belongs to one selected transport, so the transport does not need to be repeated in every swarm key.

Mix session events populate the Mix adapter. Switch events populate the direct adapter, subject to the existing relay exclusions. A physical Mix relay connection never populates the Mix application-peer table. On the recipient, MixTransport exposes the anonymous session identity; that incoming peer does not need to advertise a provider record before the protocol can reply to it.

## Sending replies from the anonymous recipient

BlockExchange normally uses an outgoing connection for presence messages. On the Mix recipient, however, the remote peer ID is a session pseudonym, not a destination that can be contacted by starting a new Mix connection. The original reference integration attempted that dial and could receive a request without being able to return its presence response.

The mounted handler now marks an incoming Mix stream as usable for outgoing messages when it starts the peer's read loop:

```nim
proc readLoop*(
    self: NetworkPeer, conn: Connection, useForSending: bool = false
) {.async: (raises: []).}
```

When `useForSending` is true and the peer has no usable outgoing connection, `readLoop` retains `conn` as the connection used by `NetworkPeer.send`. The read loop continues reading that same bidirectional stream. A presence response therefore travels back through the established Mix stream instead of attempting a new connection to the pseudonym. Block responses already use the stream on which their request arrived.

The Mix network adapter also remembers which peer IDs were introduced by recipient-side session events. If such a peer has no usable stream, the outgoing connection provider returns no connection; it does not attempt to dial the pseudonym. A later incoming stream can become the reply connection. Closing the retained stream clears the connection reference, and closing the session removes the recipient identity from the adapter.

Direct incoming streams retain their previous behavior. Each BlockExchange message or block response is submitted as one connection write; MixTransport serializes those writes before fragmenting them, so concurrently produced responses do not interleave their bytes.

## Using provider addresses

Manifest discovery and block-provider discovery both return `PeerRecord` values. A record can contain ordinary addresses and Mix advertisements. For a Mix request, `mixAddresses` decodes each advertisement and checks its embedded public key against the record's peer ID:

```nim
func mixAddresses*(
    peer: PeerId, addresses: openArray[MultiAddress]
): seq[MultiAddress] =
  for address in addresses:
    if MixPubInfo.fromMixAddress(address, Opt.some(peer)).isOk:
      result.add(address)
```

The Mix connection path rejects a provider if no validated Mix address remains. Otherwise, BlockExchange calls the address-aware `MixTransport.connect`, and manifest fetching calls the address-aware `MixTransport.dial`. The explicit destination supplies the final Mix hop; it does not have to be added to the relay pool. Subsequent streams can reuse the established session through the peer-ID overload.

The Direct path removes Mix advertisements before passing addresses to the Switch. A provider with no ordinary address is not dialed by that path. An advertisement is contact information, not a guarantee of reachability or support for the requested application protocol; connection and stream establishment still report those failures.

## Discovery and swarm admission

Discovery requests are keyed by `(CID, transport)`. Requests for the same CID over different transports can therefore both establish their intended connection type. This key controls provider dialing, not the DHT lookup mechanism itself.

After dialing the returned providers, the discovery engine calls `onProviders`. The BlockExchange engine records the connected providers for matching active downloads in `DownloadContext.providerPeers`. The worker's `candidatePeers` operation considers these providers first.

A Mix download probes only these content-specific providers. A Direct download retains the existing fallback of probing other direct peers when considering candidates, after known providers. This preserves existing direct transfers between connected nodes while preventing unrelated Mix sessions from becoming a Mix download's initial swarm. More sophisticated replacement of low-value direct candidates in a full swarm remains follow-up work.

`ActiveDownload.addPeerIfAbsent` now returns the result of `Swarm.addPeer`. If the swarm rejects the peer because it is full or the peer was removed, the caller does not send a presence request as though admission succeeded.

Presence responses arrive through a transport-specific handler. The engine applies a response only to a download using that transport, and shares availability with other downloads of the same tree only within that transport. A response received directly cannot populate a Mix download's swarm.

## Streaming reads and shared local content

The streaming endpoint creates a download and returns a `StoreStream` that reads blocks as they become available. Previously, `NetworkStore.getBlock` selected an arbitrary active download for the tree CID when it needed to wait for a missing block. That selection is ambiguous when two downloads of the same tree use different transports.

The streaming path now creates a lightweight `NetworkStore` view with the new download's ID:

```nim
proc new*(
    T: type NetworkStore,
    engine: BlockExcEngine,
    localStore: BlockStore,
    downloadId: Option[uint64] = none(uint64),
): NetworkStore =
  NetworkStore(localStore: localStore, engine: engine, downloadId: downloadId)
```

When the view needs to wait, it obtains the block handle from that specific download. It does not attach the read to another download's scheduler or cancellation state. Existing unscoped `NetworkStore` callers retain their previous behavior.

Both transports still share the local content-addressed store. A verified block already available locally can satisfy either download without another network request. The selected transport governs network connections; it does not partition cached content by the route through which the content arrived.

## Boundaries of this increment

The existing DHT proxy behavior, generic MixTransport wire protocol, and AutoNAT address-mapper ordering are unchanged. Recipient-side session reset still has the limitation described in the earlier integration walkthrough. Provider-address validation and a homogeneous swarm do not resolve stale advertisements, unreachable providers, or the previously observed high-concurrency harness stalls.

## Local verification

The focused checks cover:

- Eight network tests, including independent peer entries/departures and no direct connection-provider call when Mix is unavailable.
- Fifty-seven download-manager tests, including transport-specific background reuse and a streaming read waiting on its own download ID.
- Three transport-selection tests: parameter validation, provider identity validation, and a five-node real-Mix integration test. The integration test fetches manifests concurrently over Direct and Mix, transfers blocks over each path, checks actual stream types, and rejects a direct-only provider record for a Mix request even when a session already exists.
- The 117-test BlockExchange engine regression suite and a Storage compile-only build.

The end-to-end test uses an in-memory provider-discovery stub so the transport path is exercised without depending on a live DHT. It does not verify AutoNAT advertisement, real provider propagation, the REST endpoint over HTTP, or sustained concurrent download load. Those remain separate integration checks. Set `MIX_DOWNLOAD_TEST_LOGS=1` when running `testdownloadtransport` to print diagnostic logs.
