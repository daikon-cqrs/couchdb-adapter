<?php
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\CouchDb\Storage;

use Daikon\CouchDb\Connector\CouchDbConnector;
use Daikon\Dbal\Exception\DbalException;
use Daikon\EventSourcing\EventStore\Commit\CommitSequence;
use Daikon\EventSourcing\EventStore\Commit\CommitSequenceInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageAdapterInterface;
use GuzzleHttp\Psr7\Request;

final class CouchDbStorageAdapter implements StorageAdapterInterface
{
    /** @var CouchDbConnector */
    private $connector;

    /** @var array */
    private $settings;

    public function __construct(CouchDbConnector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function load(string $identifier): CommitSequenceInterface
    {
        $viewPath = sprintf(
            '/_design/%s/_view/%s',
            $this->settings['design_doc'],
            $this->settings['view_name'] ?? 'commit_stream'
        );

        $viewParams = [
            'startkey' => sprintf('["%s", {}]', $identifier),
            'endkey' => sprintf('["%s", 1]', $identifier),
            'include_docs' => 'true',
            'reduce' => 'false',
            'descending' => 'true',
            'limit' => 1000 // @todo use snapshot size config setting as soon as available
        ];

        $response = $this->request($viewPath, 'GET', [], $viewParams);
        $rawResponse = json_decode($response->getBody()->getContents(), true);

        if (!isset($rawResponse['total_rows'])) {
            throw new DbalException('Failed to read data for '.$identifier);
        }

        return CommitSequence::fromNative(
            array_map(
                function (array $commit): array {
                    return $commit['doc'];
                },
                array_reverse($rawResponse['rows'])
            )
        );
    }

    public function append(string $identifier, array $body): void
    {
        $response = $this->request($identifier, 'PUT', $body);
        $rawResponse = json_decode($response->getBody()->getContents(), true);

        if (!isset($rawResponse['ok']) || !isset($rawResponse['rev'])) {
            throw new DbalException('Failed to write data for '.$identifier);
        }
    }

    public function purge(string $identifier): void
    {
        throw new DbalException('Not yet implemented');
    }

    /** @return mixed */
    private function request(string $identifier, string $method, array $body = [], array $params = [])
    {
        $uri = $this->buildUri($identifier, $params);

        $request = empty($body)
            ? new Request($method, $uri)
            : new Request($method, $uri, [], json_encode($body));

        return $this->connector->getConnection()->send($request);
    }

    private function buildUri(string $identifier, array $params = []): string
    {
        $settings = $this->connector->getSettings();
        $uri = sprintf('/%s/%s', $settings['database'], $identifier);
        if (!empty($params)) {
            $uri .= '?'.http_build_query($params);
        }
        return str_replace('//', '/', $uri);
    }
}
