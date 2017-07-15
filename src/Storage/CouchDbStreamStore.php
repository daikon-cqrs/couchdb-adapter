<?php

namespace Daikon\CouchDb\Storage;

use Daikon\EventSourcing\Aggregate\AggregateRevision;
use Daikon\EventSourcing\EventStore\StoreResultInterface;
use Daikon\EventSourcing\EventStore\StoreSuccess;
use Daikon\EventSourcing\EventStore\Stream;
use Daikon\EventSourcing\EventStore\StreamId;
use Daikon\EventSourcing\EventStore\StreamInterface;
use Daikon\EventSourcing\EventStore\StreamRevision;
use Daikon\EventSourcing\EventStore\StreamStoreInterface;

final class CouchDbStreamStore implements StreamStoreInterface
{
    private $storageAdapter;

    public function __construct(CouchDbStorageAdapter $storageAdapter)
    {
        $this->storageAdapter = $storageAdapter;
    }

    public function checkout(
        StreamId $streamId,
        AggregateRevision $from = null,
        AggregateRevision $to = null
    ): StreamInterface {
        $commitSequence = $this->storageAdapter->read($streamId->toNative());
        return new Stream($streamId, $commitSequence);
    }

    public function commit(StreamInterface $stream, StreamRevision $knownHead): StoreResultInterface
    {
        $commitSequence = $stream->getCommitRange($knownHead->increment(), $stream->getStreamRevision());
        foreach ($commitSequence as $commit) {
            $identifier = $stream->getStreamId()->toNative().'-'.$commit->getStreamRevision();
            $this->storageAdapter->write($identifier, $commit->toArray());
        }
        return new StoreSuccess;
    }
}
