<?php

declare(strict_types=1);

namespace NsqPHP\NsqBundle\Messenger;

use Nsq\Subscriber;
use Nsq\Writer;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use function parse_str;
use function parse_url;
use function sprintf;
use function strpos;

final class NsqTransportFactory implements TransportFactoryInterface
{
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
            new Writer($address),
            new Subscriber($address),
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
        return 0 === strpos($dsn, 'nsq://');
    }
}
