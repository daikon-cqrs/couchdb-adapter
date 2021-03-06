<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\CouchDb\Storage;

use Daikon\Dbal\Exception\DocumentConflict;
use Daikon\EventSourcing\Aggregate\AggregateIdInterface;
use Daikon\EventSourcing\Aggregate\AggregateRevision;
use Daikon\EventSourcing\EventStore\Commit\CommitInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageError;
use Daikon\EventSourcing\EventStore\Storage\StorageResultInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageSuccess;
use Daikon\EventSourcing\EventStore\Storage\StreamStorageInterface;
use Daikon\EventSourcing\EventStore\Stream\Sequence;
use Daikon\EventSourcing\EventStore\Stream\Stream;
use Daikon\EventSourcing\EventStore\Stream\StreamInterface;

final class CouchDbStreamStorage implements StreamStorageInterface
{
    private CouchDbStorageAdapter $storageAdapter;

    public function __construct(CouchDbStorageAdapter $storageAdapter)
    {
        $this->storageAdapter = $storageAdapter;
    }

    public function load(
        AggregateIdInterface $aggregateId,
        AggregateRevision $from = null,
        AggregateRevision $to = null
    ): StreamInterface {
        $commitSequence = $this->storageAdapter->load((string)$aggregateId, (string)$from, (string)$to);

        return Stream::fromNative([
            'aggregateId' => $aggregateId->toNative(),
            'commitSequence' => $commitSequence->toNative()
        ]);
    }

    public function append(StreamInterface $stream, Sequence $knownHead): StorageResultInterface
    {
        $commitSequence = $stream->getCommitRange($knownHead->increment(), $stream->getHeadSequence());

        try {
            /** @var CommitInterface $commit */
            foreach ($commitSequence as $commit) {
                $identifier = $stream->getAggregateId().'-'.(string)$commit->getSequence();
                $this->storageAdapter->append($identifier, $commit->toNative());
            }
        } catch (DocumentConflict $error) {
            return new StorageError;
        }

        return new StorageSuccess;
    }
}
