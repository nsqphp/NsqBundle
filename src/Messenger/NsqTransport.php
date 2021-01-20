<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Generator;
use JsonException;
use LogicException;
use Nsq\Envelope as NsqEnvelope;
use Nsq\Subscriber;
use Nsq\Writer;
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
    private Writer $writer;

    private Subscriber $subscriber;

    private SerializerInterface $serializer;

    private ?Generator $generator = null;

    private string $topic;

    private string $channel;

    public function __construct(
        Writer $writer,
        Subscriber $subscriber,
        string $topic,
        string $channel,
        SerializerInterface $serializer = null
    ) {
        $this->writer = $writer;
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

        $this->writer->pub($this->topic, json_encode($encodedMessage, JSON_THROW_ON_ERROR));

        return $envelope;
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        $generator = $this->generator;
        if (null === $generator) {
            $this->generator = $generator = $this->subscriber->subscribe($this->topic, $this->channel);
        } else {
            $generator->next();
        }

        /** @var NsqEnvelope|null $nsqEnvelope */
        $nsqEnvelope = $generator->current();

        if (null === $nsqEnvelope) {
            return [];
        }

        try {
            $encodedEnvelope = json_decode($nsqEnvelope->message->body, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            $nsqEnvelope->finish();

            throw new MessageDecodingFailedException('', 0, $e);
        }

        try {
            $envelope = $this->serializer->decode($encodedEnvelope);
        } catch (MessageDecodingFailedException  $e) {
            $nsqEnvelope->finish();

            throw $e;
        }

        return [
            $envelope->with(
                new NsqReceivedStamp($nsqEnvelope),
                new TransportMessageIdStamp($nsqEnvelope->message->id),
            ),
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        $message = $this->getNsqEnvelope($envelope);
        if (!$message instanceof NsqEnvelope) {
            throw new LogicException('Returned envelop doesn\'t related to NsqMessage.');
        }

        $message->finish();
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        $message = $this->getNsqEnvelope($envelope);
        if (!$message instanceof NsqEnvelope) {
            throw new LogicException('Returned envelop doesn\'t related to NsqMessage.');
        }

        $message->finish();
    }

    private function getNsqEnvelope(Envelope $envelope): ?NsqEnvelope
    {
        $stamp = $envelope->last(NsqReceivedStamp::class);
        if (!$stamp instanceof NsqReceivedStamp) {
            return null;
        }

        return $stamp->envelope;
    }
}
