<?php

namespace Daikon\CouchDb\Storage;

use Daikon\EventSourcing\EventStore\CommitStream;
use Daikon\EventSourcing\EventStore\CommitStreamId;
use Daikon\EventSourcing\EventStore\CommitStreamInterface;
use Daikon\EventSourcing\EventStore\CommitStreamRevision;
use Daikon\EventSourcing\EventStore\StoreResultInterface;
use Daikon\EventSourcing\EventStore\StoreSuccess;
use Daikon\EventSourcing\EventStore\StreamStoreInterface;

final class CouchDbStreamStore implements StreamStoreInterface
{
    private $storageAdapter;

    public function __construct(CouchDbStorageAdapter $storageAdapter)
    {
        $this->storageAdapter = $storageAdapter;
    }

    public function checkout(
        CommitStreamId $streamId,
        CommitStreamRevision $from = null,
        CommitStreamRevision $to = null
    ): CommitStreamInterface {
        $commitSequence = $this->storageAdapter->read($streamId->toNative());
        return new CommitStream($streamId, $commitSequence);
    }

    public function commit(CommitStreamInterface $stream, CommitStreamRevision $storeHead): StoreResultInterface
    {
        $commitSequence = $stream->getCommitRange($storeHead, $stream->getStreamRevision());
        foreach ($commitSequence as $commit) {
            $identifier = $stream->getStreamId()->toNative().'-'.$commit->getStreamRevision();
            $this->storageAdapter->write($identifier, $commit->toArray());
        }
        return new StoreSuccess;
    }
}
