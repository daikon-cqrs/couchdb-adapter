<?php

namespace Daikon\CouchDb\Storage;

use Daikon\EventSourcing\Aggregate\AggregateRevision;
use Daikon\EventSourcing\EventStore\Commit\CommitInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageResultInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageSuccess;
use Daikon\EventSourcing\EventStore\Storage\StreamStorageInterface;
use Daikon\EventSourcing\EventStore\Stream\Stream;
use Daikon\EventSourcing\EventStore\Stream\StreamIdInterface;
use Daikon\EventSourcing\EventStore\Stream\StreamInterface;
use Daikon\EventSourcing\EventStore\Stream\StreamRevision;

final class CouchDbStreamStore implements StreamStorageInterface
{
    private $storageAdapter;

    public function __construct(CouchDbStorageAdapter $storageAdapter)
    {
        $this->storageAdapter = $storageAdapter;
    }

    public function checkout(
        StreamIdInterface $streamId,
        AggregateRevision $from = null,
        AggregateRevision $to = null
    ): StreamInterface {
        $commitSequence = $this->storageAdapter->read($streamId->toNative());
        return new Stream($streamId, $commitSequence);
    }

    public function commit(StreamInterface $stream, StreamRevision $knownHead): StorageResultInterface
    {
        $commitSequence = $stream->getCommitRange($knownHead->increment(), $stream->getStreamRevision());
        /** @var CommitInterface $commit */
        foreach ($commitSequence as $commit) {
            $identifier = $stream->getStreamId()->toNative().'-'.$commit->getStreamRevision();
            $this->storageAdapter->write($identifier, $commit->toArray());
        }
        return new StorageSuccess;
    }
}
