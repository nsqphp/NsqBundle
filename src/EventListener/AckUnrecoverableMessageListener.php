<?php
declare(strict_types=1);

namespace Nsq\NsqBundle\EventListener;

use Nsq\NsqBundle\Messenger\NsqReceivedStamp;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use function Amp\Promise\wait;

final class AckUnrecoverableMessageListener implements EventSubscriberInterface
{
    /**
     * {@inheritdoc}
     */
    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageFailedEvent::class => ['onMessageFailed', 500],
        ];
    }

    public function onMessageFailed(WorkerMessageFailedEvent $event): void
    {
        if ($event->willRetry()) {
            return;
        }

        $envelope = $event->getEnvelope();
        $message = NsqReceivedStamp::getMessageFromEnvelope($envelope);

        if ($message->isProcessed()) {
            return;
        }

        wait($message->finish());
    }
}
