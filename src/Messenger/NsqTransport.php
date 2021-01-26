<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Generator;
use JsonException;
use LogicException;
use Nsq\Producer;
use Nsq\Subscriber;
use Nsq\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use function json_decode;
use function json_encode;
use const JSON_THROW_ON_ERROR;

final class NsqTransport implements TransportInterface
{
    private Producer $producer;

    private Subscriber $subscriber;

    private SerializerInterface $serializer;

    private ?Generator $generator = null;

    private string $topic;

    private string $channel;

    public function __construct(
        Producer $producer,
        Subscriber $subscriber,
        string $topic,
        string $channel,
        SerializerInterface $serializer = null,
    ) {
        $this->producer = $producer;
        $this->subscriber = $subscriber;
        $this->topic = $topic;
        $this->channel = $channel;
        $this->serializer = $serializer ?? new PhpSerializer();
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
        $this->producer->receive(); // keepalive, handle heartbeat messages

        $generator = $this->generator;
        if (null === $generator) {
            $this->generator = $generator = $this->subscriber->subscribe($this->topic, $this->channel);
        } else {
            $generator->next();
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
        $message = $this->getMessage($envelope);
        if (!$message instanceof Message) {
            throw new LogicException('Returned envelop doesn\'t related to NsqMessage.');
        }

        $message->finish();
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        $message = $this->getMessage($envelope);
        if (!$message instanceof Message) {
            throw new LogicException('Returned envelop doesn\'t related to NsqMessage.');
        }

        $message->finish();
    }

    private function getMessage(Envelope $envelope): ?Message
    {
        $stamp = $envelope->last(NsqReceivedStamp::class);
        if (!$stamp instanceof NsqReceivedStamp) {
            return null;
        }

        return $stamp->message;
    }
}
