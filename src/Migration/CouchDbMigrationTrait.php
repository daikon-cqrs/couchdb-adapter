<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\CouchDb\Migration;

use Daikon\Dbal\Exception\DbalException;
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
                throw new DbalException("Failed to create database '$database'.");
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
                throw new DbalException("Failed to delete database '$database'.");
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

        $requestPath = sprintf('/%s/_design/%s', $database, $name);
        $client->put($requestPath, ['body' => json_encode($body)]);
    }

    private function deleteDesignDoc(string $database, string $name): void
    {
        $client = $this->connector->getConnection();
        $requestPath = sprintf('/%s/_design/%s', $database, $name);
        $response = $client->head($requestPath);
        $revision = trim(current($response->getHeader('ETag')), '"');
        $client->delete(sprintf('%s?rev=%s', $requestPath, $revision));
    }

    private function getDatabaseName(): string
    {
        $connectorSettings = $this->connector->getSettings();
        return $connectorSettings['database'];
    }
}
