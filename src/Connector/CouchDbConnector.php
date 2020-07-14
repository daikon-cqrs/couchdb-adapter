<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/couchdb-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\CouchDb\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ProvidesConnector;
use GuzzleHttp\Client;

final class CouchDbConnector implements ConnectorInterface
{
    use ProvidesConnector;

    protected function connect(): Client
    {
        $clientOptions = [
            'base_uri' => sprintf(
                '%s://%s:%s',
                $this->settings['scheme'],
                $this->settings['host'],
                $this->settings['port']
            )
        ];

        if (isset($this->settings['debug'])) {
            $clientOptions['debug'] = $this->settings['debug'] === true;
        }

        if (isset($this->settings['user']) && !empty($this->settings['user'])
            && isset($this->settings['password']) && !empty($this->settings['password'])
        ) {
            $clientOptions['auth'] = [
                $this->settings['user'],
                $this->settings['password'],
                $this->settings['authentication'] ?? 'basic'
            ];
        }

        $clientOptions['headers'] = [
            'Accept' => 'application/json',
            'Content-Type' => 'application/json'
        ];

        return new Client($clientOptions);
    }
}
