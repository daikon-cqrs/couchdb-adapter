<?php

namespace Daikon\CouchDb\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ConnectorTrait;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Uri;
use Psr\Http\Message\RequestInterface;

final class CouchDbConnector implements ConnectorInterface
{
    use ConnectorTrait;

    private function connect()
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

        if (isset($this->settings['username']) && isset($this->settings['password'])
        ) {
            $clientOptions['auth'] = [
                $this->settings['username'],
                $this->settings['password'],
                $this->settings['authentication'] ?? 'basic'
            ];
        }

        if (isset($this->settings['default_headers'])) {
            $clientOptions['headers'] = $this->settings['default_headers'];
        }

        if (isset($this->settings['default_options'])) {
            $clientOptions = array_merge($clientOptions, $this->settings['default_options']);
        }

        if (isset($this->settings['default_query'])) {
            $handler = HandlerStack::create();
            $handler->push(Middleware::mapRequest(
                function (RequestInterface $request) {
                    $uri = $request->getUri();
                    foreach ($this->settings['default_query'] as $param => $value) {
                        $uri = Uri::withQueryValue($uri, $param, $value);
                    }
                    return $request->withUri($uri);
                }
            ));
            $clientOptions['handler'] = $handler;
        }

        return new Client($clientOptions);
    }
}
