<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\CouchDb\Migration;

use Daikon\CouchDb\Connector\CouchDbConnector;
use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Exception\DbalException;
use Daikon\Dbal\Migration\MigrationAdapterInterface;
use Daikon\Dbal\Migration\MigrationList;
use DateTimeImmutable;
use GuzzleHttp\Exception\BadResponseException;
use GuzzleHttp\Psr7\Request;

final class CouchDbMigrationAdapter implements MigrationAdapterInterface
{
    private CouchDbConnector $connector;

    private array $settings;

    public function __construct(CouchDbConnector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier): MigrationList
    {
        try {
            $response = $this->request($identifier, 'GET');
            $rawResponse = json_decode((string)$response->getBody(), true);
        } catch (BadResponseException $error) {
            /** @psalm-suppress PossiblyNullReference */
            if ($error->hasResponse() && $error->getResponse()->getStatusCode() === 404) {
                return new MigrationList;
            }
            throw new DbalException("Failed to read migrations for '$identifier'.");
        }

        return $this->createMigrationList($rawResponse['migrations']);
    }

    public function write(string $identifier, MigrationList $migrationList): void
    {
        if ($migrationList->isEmpty()) {
            return;
        }

        $body = [
            'target' => $identifier,
            'migrations' => $migrationList->toNative()
        ];

        if ($revision = $this->getCurrentRevision($identifier)) {
            $body['_rev'] = $revision;
        }

        $response = $this->request($identifier, 'PUT', $body);
        $rawResponse = json_decode((string)$response->getBody(), true);

        if (!isset($rawResponse['ok']) || !isset($rawResponse['rev'])) {
            throw new DbalException("Failed to write migrations for '$identifier'.");
        }
    }

    public function getConnector(): ConnectorInterface
    {
        return $this->connector;
    }

    private function createMigrationList(array $migrationData): MigrationList
    {
        $migrations = [];
        foreach ($migrationData as $migration) {
            $migrationClass = $migration['@type'];
            /*
             * Explicitly not using a service locator to make migration classes here because
             * it could enable unusual behaviour.
             */
            $migrations[] = new $migrationClass(new DateTimeImmutable($migration['executedAt']));
        }
        return (new MigrationList($migrations))->sortByVersion();
    }

    private function getCurrentRevision(string $identifier): ?string
    {
        $revision = null;

        try {
            $response = $this->request($identifier, 'HEAD');
            $revision = trim(current($response->getHeader('ETag')), '"');
        } catch (BadResponseException $error) {
            /** @psalm-suppress PossiblyNullReference */
            if (!$error->hasResponse() || $error->getResponse()->getStatusCode() !== 404) {
                throw new DbalException("Failed to get current migration for '$identifier'.");
            }
        }

        return $revision;
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
