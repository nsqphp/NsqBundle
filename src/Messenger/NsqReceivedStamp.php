<?php

declare(strict_types=1);

namespace Nsq\NsqBundle\Messenger;

use Nsq\Envelope;
use Symfony\Component\Messenger\Stamp\StampInterface;

/**
 * @psalm-immutable
 */
final class NsqReceivedStamp implements StampInterface
{
    public Envelope $envelope;

    public function __construct(Envelope $envelope)
    {
        $this->envelope = $envelope;
    }
}
