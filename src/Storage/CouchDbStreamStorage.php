<?php

/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\CouchDb\Storage;

use Daikon\EventSourcing\Aggregate\AggregateIdInterface;
use Daikon\EventSourcing\Aggregate\AggregateRevision;
use Daikon\EventSourcing\EventStore\Commit\CommitInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageResultInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageSuccess;
use Daikon\EventSourcing\EventStore\Storage\StreamStorageInterface;
use Daikon\EventSourcing\EventStore\Stream\Sequence;
use Daikon\EventSourcing\EventStore\Stream\Stream;
use Daikon\EventSourcing\EventStore\Stream\StreamInterface;

final class CouchDbStreamStorage implements StreamStorageInterface
{
    /** @var CouchDbStorageAdapter */
    private $storageAdapter;

    public function __construct(CouchDbStorageAdapter $storageAdapter)
    {
        $this->storageAdapter = $storageAdapter;
    }

    public function load(
        AggregateIdInterface $aggregateId,
        AggregateRevision $from = null,
        AggregateRevision $to = null
    ): StreamInterface {
        $commitSequence = $this->storageAdapter->load((string) $aggregateId);
        return Stream::fromNative([
            'aggregateId' => $aggregateId,
            'commitSequence' => $commitSequence
        ]);
    }

    public function append(StreamInterface $stream, Sequence $knownHead): StorageResultInterface
    {
        $commitSequence = $stream->getCommitRange($knownHead->increment(), $stream->getSequence());
        /** @var CommitInterface $commit */
        foreach ($commitSequence as $commit) {
            $identifier = $stream->getAggregateId() . '-' . $commit->getSequence();
            $this->storageAdapter->append($identifier, $commit->toNative());
        }
        return new StorageSuccess;
    }
}
