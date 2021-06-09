<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Nsq\Config\ClientConfig;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class NsqTransport implements TransportInterface
{
    private ?NsqReceiver $receiver = null;

    private ?NsqSender $sender = null;

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
        return ($this->receiver ?? $this->getReceiver())->get();
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        ($this->receiver ?? $this->getReceiver())->ack($envelope);
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        ($this->receiver ?? $this->getReceiver())->reject($envelope);
    }

    /**
     * {@inheritdoc}
     */
    public function send(Envelope $envelope): Envelope
    {
        return ($this->sender ?? $this->getSender())->send($envelope);
    }

    private function getReceiver(): NsqReceiver
    {
        return $this->receiver = new NsqReceiver(
            $this->address,
            $this->topic,
            $this->channel,
            $this->clientConfig,
            $this->serializer,
            $this->logger,
        );
    }

    private function getSender(): NsqSender
    {
        return $this->sender = new NsqSender(
            $this->address,
            $this->topic,
            $this->clientConfig,
            $this->serializer,
            $this->logger,
        );
    }
}
