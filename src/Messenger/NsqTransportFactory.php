<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Nsq\Consumer;
use Nsq\Producer;
use Nsq\Subscriber;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use function parse_str;
use function parse_url;
use function sprintf;

final class NsqTransportFactory implements TransportFactoryInterface
{
    use LoggerAwareTrait;

    public function __construct(LoggerInterface $logger = null)
    {
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * {@inheritdoc}
     */
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (false === $parsedUrl = parse_url($dsn)) {
            throw new InvalidArgumentException(sprintf('The given Nsq DSN "%s" is invalid.', $dsn));
        }

        $nsqOptions = [];
        if (isset($parsedUrl['query'])) {
            parse_str($parsedUrl['query'], $nsqOptions);
        }

        $address = sprintf('tcp://%s:%s', $parsedUrl['host'] ?? 'nsqd', $parsedUrl['port'] ?? 4150);

        return new NsqTransport(
            new Producer(
                address: $address,
                logger: $this->logger,
            ),
            new Subscriber(
                new Consumer(
                    address: $address,
                    logger: $this->logger,
                )
            ),
            $nsqOptions['topic'] ?? 'symfony-messenger',
            $nsqOptions['channel'] ?? 'default',
            $serializer
        );
    }

    /**
     * {@inheritdoc}
     */
    public function supports(string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'nsq://');
    }
}
