<?php

namespace Daikon\CouchDb\Migration;

use Daikon\CouchDb\Connector\CouchDbConnector;
use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Exception\MigrationException;
use Daikon\Dbal\Migration\MigrationAdapterInterface;
use Daikon\Dbal\Migration\MigrationList;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Psr7\Request;

final class CouchDbMigrationAdapter implements MigrationAdapterInterface
{
    private $connector;

    private $settings;

    public function __construct(CouchDbConnector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier): MigrationList
    {
        try {
            $response = $this->request($identifier, 'GET');
            $rawResponse = json_decode($response->getBody(), true);
        } catch (RequestException $error) {
            if ($error->hasResponse() && $error->getResponse()->getStatusCode() === 404) {
                return new MigrationList;
            } else {
                throw new MigrationException($error->getMessage());
            }
        }
        return $this->createMigrationList($rawResponse['migrations']);
    }

    public function write(string $identifier, MigrationList $executedMigrations): void
    {
        $body = [
            'target' => $identifier,
            'migrations' => $executedMigrations->toArray()
        ];

        if ($revision = $this->getCurrentRevision($identifier)) {
            $body['_rev'] = $revision;
        }

        $response = $this->request($identifier, 'PUT', $body);
        $rawResponse = json_decode($response->getBody(), true);

        if (!isset($rawResponse['ok']) || !isset($rawResponse['rev'])) {
            throw new MigrationException('Failed to write migration data for '.$identifier);
        }
    }

    public function getConnector(): ConnectorInterface
    {
        return $this->connector;
    }

    private function createMigrationList(array $migrationData)
    {
        $migrations = [];
        foreach ($migrationData as $migration) {
            $migrationClass = $migration['@type'];
            /*
             * Explicitly not using a service locator to make migration classes here because
             * it could enable unusual behaviour.
             */
            $migrations[] = new $migrationClass(new \DateTimeImmutable($migration['executedAt']));
        }
        return new MigrationList($migrations);
    }

    private function request(string $identifier, string $method, array $body = [], array $params = [])
    {
        $requestPath = $this->buildRequestPath($identifier, $params);

        if (empty($body)) {
            $request = new Request($method, $requestPath, ['Accept' => 'application/json']);
        } else {
            $request = new Request(
                $method,
                $requestPath,
                ['Accept' => 'application/json', 'Content-Type' => 'application/json'],
                json_encode($body)
            );
        }

        return $this->connector->getConnection()->send($request);
    }

    private function getCurrentRevision(string $identifier): ?string
    {
        try {
            $response = $this->request($identifier, 'HEAD');
            $revision = trim(current($response->getHeader('ETag')), '"');
        } catch (RequestException $error) {
            if (!$error->hasResponse() || $error->getResponse()->getStatusCode() !== 404) {
                throw new MigrationException($error->getMessage());
            }
        }

        return $revision ?? null;
    }

    private function buildRequestPath(string $identifier, array $params = [])
    {
        $settings = $this->connector->getSettings();
        $requestPath = sprintf('/%s/%s', $settings['database'], $identifier);
        if (!empty($params)) {
            $requestPath .= '?'.http_build_query($params);
        }

        return str_replace('//', '/', $requestPath);
    }
}
