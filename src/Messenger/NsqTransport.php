<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Amp\Loop;
use Amp\Promise;
use Generator;
use JsonException;
use Nsq\Config\ClientConfig;
use Nsq\Message;
use Nsq\Producer;
use Nsq\Reader;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use function Amp\Promise\wait;
use function json_decode;
use function json_encode;
use const JSON_THROW_ON_ERROR;

final class NsqTransport implements TransportInterface
{
    private ?Producer $producer = null;

    private ?Reader $reader = null;

    /**
     * @var Promise<Message>|null
     */
    private ?Promise $deferred = null;

    public function __construct(
        private string $address,
        private string $topic,
        private string $channel,
        private ClientConfig $clientConfig,
        private SerializerInterface $serializer,
        private LoggerInterface $logger,
    ) {
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $producer = $this->getProducer();

        $encodedMessage = $this->serializer->encode($envelope->withoutAll(NsqReceivedStamp::class));
        $encodedMessage = json_encode($encodedMessage, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = null !== $delayStamp ? $delayStamp->getDelay() : null;

        if (null === $delay) {
            $promise = $producer->publish($this->topic, $encodedMessage);
        } else {
            $promise = $producer->defer($this->topic, $encodedMessage, $delay / 1000);
        }

        wait($promise);

        return $envelope;
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        $reader = $this->getReader();

        $promise = $this->deferred ??= $reader->consume();

        /** @var Message|null $message */
        $message = null;
        Loop::run(function () use (&$message, $promise): Generator {
            Loop::delay(500, static function () {
                Loop::stop();
            });

            $message = yield $promise;
        });

        if (null === $message) {
            return [];
        }

        $this->deferred = null;

        try {
            $encodedEnvelope = json_decode($message->body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            wait($message->finish());

            throw new MessageDecodingFailedException('', 0, $e);
        }

        try {
            $envelope = $this->serializer->decode($encodedEnvelope);
        } catch (MessageDecodingFailedException  $e) {
            wait($message->finish());

            throw $e;
        }

        return [
            $envelope->with(
                new NsqReceivedStamp($message),
                new TransportMessageIdStamp($message->id),
            ),
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        $message = NsqReceivedStamp::getMessageFromEnvelope($envelope);

        wait($message->finish());
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        $message = NsqReceivedStamp::getMessageFromEnvelope($envelope);

        wait($message->finish());
    }

    private function getProducer(): Producer
    {
        if (null === $this->producer) {
            $this->producer = new Producer(
                $this->address,
                $this->clientConfig,
                $this->logger,
            );
        }

        wait($this->producer->connect());

        return $this->producer;
    }

    private function getReader(): Reader
    {
        if (null === $this->reader) {
            $this->reader = new Reader(
                $this->address,
                $this->topic,
                $this->channel,
                $this->clientConfig,
                $this->logger,
            );
        }

        wait($this->reader->connect());

        return $this->reader;
    }
}
