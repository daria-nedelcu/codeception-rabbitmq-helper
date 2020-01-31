<?php
namespace Codeception\Module;

use Codeception\Lib\ModuleContainer;
use Codeception\Module;
use GuzzleHttp\Client;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;

class Rabbit extends Module
{
    /** @var AMQPStreamConnection[] */
    private $connections;

    public function __construct(ModuleContainer $container, array $config = [])
    {
        parent::__construct($container, $config);

        foreach ($config['connections'] as $connectionName => $connectionConfiguration) {
            $this->connections[$connectionName] = $this->createConnection($connectionConfiguration);

            foreach ($connectionConfiguration['config'] as $exchange => $exchangeConfig) {
                $this->declareExchangeAndQueue($connectionName, $exchange, $exchangeConfig);
            }
        }
    }

    private function declareExchangeAndQueue(string $connectionName, string $exchange, array $exchangeConfig)
    {
        $queue = $exchangeConfig['queue'];
        $exchangeType = $exchangeConfig['type'];

        try {
            $this->getChannel($connectionName)->queue_purge($queue);
        } catch (AMQPProtocolChannelException $e) {}

        try {
            $this->createBindings($connectionName, $exchange, $queue, $exchangeType);
        } catch (AMQPProtocolChannelException $e) {
            $this->getChannel($connectionName)->exchange_delete($exchange);
            $this->getChannel($connectionName)->queue_delete($queue);
            $this->createBindings($connectionName, $exchange, $queue, $exchangeType);
        }
    }

    private function createBindings(string $connectionName, string $exchange, string $queue, string $exchangeType)
    {
        $this->getChannel($connectionName)->queue_declare(
            $queue, false, true, false, false
        );
        $this->getChannel($connectionName)->exchange_declare(
            $exchange, $exchangeType, false, true, false
        );
        $this->getChannel($connectionName)->queue_bind($queue, $exchange);
    }

    private function getChannel(string $connectionName)
    {
        return $this->connections[$connectionName]->channel(
            $this->connections[$connectionName]->get_free_channel_id()
        );
    }

    private function createConnection(array $connectionConfiguration) : AMQPStreamConnection
    {
        $this->createVhost($connectionConfiguration);

        $connection = new AMQPStreamConnection(
            $connectionConfiguration['host'],
            $connectionConfiguration['port'],
            $connectionConfiguration['username'],
            $connectionConfiguration['password'],
            $connectionConfiguration['vhost']
        );

        return $connection;
    }

    private function createVhost(array $connectionConfiguration)
    {
        $client = new Client([
            'base_uri' => "http://{$connectionConfiguration['host']}:15672/"
        ]);

        $client->put(
            "/api/vhosts/{$connectionConfiguration['vhost']}",
            [
                'auth' => [
                    $connectionConfiguration['username'],
                    $connectionConfiguration['password']
                ],
                'headers' => [
                    'Content-Type' => 'application/json'
                ]
            ]
        );

        $client->put(
            "/api/permissions/{$connectionConfiguration['vhost']}/{$connectionConfiguration['username']}",
            [
                'auth' => [
                    $connectionConfiguration['username'],
                    $connectionConfiguration['password']
                ],
                'headers' => [
                    'Content-Type' => 'application/json'
                ],
                'json' => ["configure" => ".*", "write" => ".*", "read" => ".*"]
            ]
        );
    }

    public function pushToExchange(string $exchange, string $content, string $connection, string $routingKey = null)
    {
        $this->assertArrayHasKey(
            $connection, $this->connections, "There is no AMQP connection with name $connection"
        );

        $this->getChannel($connection)->basic_publish(
            new AMQPMessage($content), $exchange, $routingKey
        );
    }

    public function grabMessageFromQueue(string $queue, string $connection): ? string
    {
        $this->assertArrayHasKey(
            $connection, $this->connections, "There is no AMQP connection with name $connection"
        );
        /** @var AMQPMessage $message */
        $message = $this->getChannel($connection)->basic_get($queue, true);

        return $message ? $message->getBody() : null;
    }

    public function purgeQueue(string $queue, string $connectionName)
    {
        $this->getChannel($connectionName)->queue_purge($queue);
    }

    public function checkQueueIsEmpty(string $queue, string $connectionName)
    {
        $this->assertNull($this->grabMessageFromQueue($queue, $connectionName));
    }
}
