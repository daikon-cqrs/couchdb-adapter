<?php

namespace Daikon\CouchDb\Migration;

use Daikon\Dbal\Exception\MigrationException;
use Daikon\Dbal\Migration\MigrationTrait;
use GuzzleHttp\Exception\RequestException;

trait CouchDbMigrationTrait
{
    use MigrationTrait;

    private function createDatabase(string $database): void
    {
        $client = $this->connector->getConnection();

        try {
            $client->put('/'.$database);
        } catch (RequestException $error) {
            if (!$error->hasResponse() || !$error->getResponse()->getStatusCode() === 409) {
                throw new MigrationException($error->getMessage());
            }
        }
    }

    private function deleteDatabase(string $database): void
    {
        $client = $this->connector->getConnection();

        try {
            $client->delete('/'.$database);
        } catch (RequestException $error) {
            if (!$error->hasResponse() || !$error->getResponse()->getStatusCode() === 404) {
                throw new MigrationException($error->getMessage());
            }
        }
    }

    private function createDesignDoc(string $database, string $name, array $views): void
    {
        $client = $this->connector->getConnection();

        $body = [
            'language' => 'javascript',
            'views' => $views
        ];

        try {
            $requestPath = sprintf('/%s/_design/%s', $database, $name);
            $client->put($requestPath, ['body' => json_encode($body)]);
        } catch (RequestException $error) {
            throw new MigrationException($error->getMessage());
        }
    }

    private function deleteDesignDoc(string $database, string $name): void
    {
        $client = $this->connector->getConnection();

        try {
            $requestPath = sprintf('/%s/_design/%s', $database, $name);
            $response = $client->head($requestPath);
            $revision = trim(current($response->getHeader('ETag')), '"');
            $client->delete(sprintf('%s?rev=%s', $requestPath, $revision));
        } catch (RequestException $error) {
            throw new MigrationException($error->getMessage());
        }
    }

    private function getDatabaseName(): string
    {
        $connectorSettings = $this->connector->getSettings();
        return $connectorSettings['database'];
    }
}
