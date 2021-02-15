<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Generator;
use JsonException;
use Nsq\Message;
use Nsq\Producer;
use Nsq\Subscriber;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Throwable;
use function json_decode;
use function json_encode;
use const JSON_THROW_ON_ERROR;

final class NsqTransport implements TransportInterface
{
    private SerializerInterface $serializer;

    private ?LoggerInterface $logger;

    private ?Generator $generator = null;

    public function __construct(
        private Producer $producer,
        private Subscriber $subscriber,
        private string $topic,
        private string $channel,
        SerializerInterface $serializer = null,
        LoggerInterface $logger = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
        $this->logger = $logger;
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope->withoutAll(NsqReceivedStamp::class));

        $this->producer->pub($this->topic, json_encode($encodedMessage, JSON_THROW_ON_ERROR));

        return $envelope;
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        try {
            $this->producer->receive(0); // keepalive, handle heartbeat messages
        } catch (Throwable $e) {
            $this->logger->error('Producer keepalive failed.', ['exception' => $e]);
        }

        $generator = $this->generator;
        if (null === $generator) {
            $this->generator = $generator = $this->subscriber->subscribe($this->topic, $this->channel);
        } else {
            try {
                $generator->next();
            } catch (Throwable $e) {
                $this->logger->error('Consumer next failed.', ['exception' => $e]);

                return [];
            }
        }

        /** @var Message|null $nsqMessage */
        $nsqMessage = $generator->current();

        if (null === $nsqMessage) {
            return [];
        }

        try {
            $encodedEnvelope = json_decode($nsqMessage->body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            $nsqMessage->finish();

            throw new MessageDecodingFailedException('', 0, $e);
        }

        try {
            $envelope = $this->serializer->decode($encodedEnvelope);
        } catch (MessageDecodingFailedException  $e) {
            $nsqMessage->finish();

            throw $e;
        }

        return [
            $envelope->with(
                new NsqReceivedStamp($nsqMessage),
                new TransportMessageIdStamp($nsqMessage->id),
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
