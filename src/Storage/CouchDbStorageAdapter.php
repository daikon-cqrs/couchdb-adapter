<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\CouchDb\Storage;

use Daikon\CouchDb\Connector\CouchDbConnector;
use Daikon\Dbal\Exception\DbalException;
use Daikon\Dbal\Exception\DocumentConflict;
use Daikon\EventSourcing\EventStore\Commit\CommitSequence;
use Daikon\EventSourcing\EventStore\Commit\CommitSequenceInterface;
use Daikon\EventSourcing\EventStore\Storage\StorageAdapterInterface;
use GuzzleHttp\Exception\BadResponseException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

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

    public function load(string $identifier, string $from = null, string $to = null): CommitSequenceInterface
    {
        $viewPath = sprintf(
            '/_design/%s/_view/%s',
            $this->settings['design_doc'],
            $this->settings['view_name'] ?? 'commit_stream'
        );

        $viewParams = [
            'startkey' => sprintf('["%s", %s]', $identifier, $from ?: '{}'),
            'endkey' => sprintf('["%s", %s]', $identifier, $to ?: 1),
            'include_docs' => 'true',
            'reduce' => 'false',
            'descending' => 'true',
            'limit' => 5000 //@todo use snapshot size config setting as soon as available
        ];

        /** @var Response $response */
        $response = $this->request($viewPath, 'GET', [], $viewParams);
        $rawResponse = json_decode((string)$response->getBody(), true);

        if (!isset($rawResponse['rows'])) {
            //@todo add error logging
            throw new DbalException('Failed to load data for '.$identifier);
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
        /** @var Response $response */
        $response = $this->request($identifier, 'PUT', $body);

        if ($response->getStatusCode() === 409) {
            throw new DocumentConflict;
        }

        $rawResponse = json_decode((string)$response->getBody(), true);
        if (!isset($rawResponse['ok']) || !isset($rawResponse['rev'])) {
            //@todo add error logging
            throw new DbalException('Failed to append data for '.$identifier);
        }
    }

    public function purge(string $identifier): void
    {
        throw new DbalException('Not implemented');
    }

    /** @return mixed */
    private function request(string $identifier, string $method, array $body = [], array $params = [])
    {
        $uri = $this->buildUri($identifier, $params);

        $request = empty($body)
            ? new Request($method, $uri)
            : new Request($method, $uri, [], json_encode($body));

        try {
            $response = $this->connector->getConnection()->send($request);
        } catch (BadResponseException $error) {
            $response = $error->getResponse();
        }

        return $response;
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
