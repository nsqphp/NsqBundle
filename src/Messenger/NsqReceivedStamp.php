<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use LogicException;
use Nsq\Message;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * @psalm-immutable
 */
final class NsqReceivedStamp implements StampInterface
{
    public Message $message;

    public function __construct(Message $message)
    {
        $this->message = $message;
    }

    public static function getMessageFromEnvelope(Envelope $envelope): Message
    {
        /** @var self|null $stamp */
        $stamp = $envelope->last(self::class);

        if (null === $stamp) {
            throw new LogicException('Envelop doesn\'t related to NsqMessage.');
        }

        return $stamp->message;
    }
}
