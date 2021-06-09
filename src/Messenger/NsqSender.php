<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Nsq\Config\ClientConfig;
use Nsq\Producer;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use function Amp\Promise\wait;
use function json_encode;
use const JSON_THROW_ON_ERROR;

final class NsqSender implements SenderInterface
{
    private ?Producer $producer = null;

    public function __construct(
        private string $address,
        private string $topic,
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
            $promise = $producer->defer($this->topic, $encodedMessage, $delay);
        }

        wait($promise);

        return $envelope;
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
}
