<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use JsonException;
use Nsq\Config\ClientConfig;
use Nsq\Consumer;
use Nsq\Message;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use function Amp\delay;
use function Amp\Promise\wait;
use function array_pop;
use function json_decode;
use const JSON_THROW_ON_ERROR;

final class NsqReceiver implements ReceiverInterface
{
    private ?Consumer $consumer = null;

    /**
     * @var Message[]
     */
    private array $messages = [];

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
    public function get(): iterable
    {
        if ([] === $this->messages) {
            $this->consume();

            wait(delay(500));
        }

        $message = array_pop($this->messages);

        if (null === $message) {
            return [];
        }

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
                new RedeliveryStamp($message->attempts - 1),
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

        if ($message->isProcessed()) {
            return;
        }

        wait($message->finish());
    }

    private function consume(): void
    {
        if (null === $this->consumer) {
            $this->consumer = new Consumer(
                $this->address,
                $this->topic,
                $this->channel,
                function (Message $message) {
                    $this->messages[] = $message;
                },
                $this->clientConfig,
                $this->logger,
            );
        }

        wait($this->consumer->connect());
    }
}
