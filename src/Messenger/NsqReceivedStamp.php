<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Nsq\Message;
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
}
