<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Amp\Loop;
use Amp\Promise;
use Generator;
use JsonException;
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
    /**
     * @var Promise<Message>|null
     */
    private ?Promise $deferred = null;

    public function __construct(
        private Producer $producer,
        private Reader $reader,
        private string $topic,
        private SerializerInterface $serializer,
        private LoggerInterface $logger,
    ) {
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope->withoutAll(NsqReceivedStamp::class));
        $encodedMessage = json_encode($encodedMessage, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = null !== $delayStamp ? $delayStamp->getDelay() : null;

        if (null === $delay) {
            $promise = $this->producer->publish($this->topic, $encodedMessage);
        } else {
            $promise = $this->producer->defer($this->topic, $encodedMessage, $delay / 1000);
        }

        wait($promise);

        return $envelope;
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        $promise = $this->deferred ??= $this->reader->consume();

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

        try {
            $encodedEnvelope = json_decode($message->body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            $message->finish();

            throw new MessageDecodingFailedException('', 0, $e);
        }

        try {
            $envelope = $this->serializer->decode($encodedEnvelope);
        } catch (MessageDecodingFailedException  $e) {
            $message->finish();

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

        $message->finish();
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        $message = NsqReceivedStamp::getMessageFromEnvelope($envelope);

        $message->finish();
    }
}
