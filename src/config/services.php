<?php

declare(strict_types=1);

use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $configurator): void {
    $services = $configurator->services();

    $services->set(Nsq\NsqBundle\Messenger\NsqTransportFactory::class)
        ->tag('messenger.transport_factory')
        ->tag('container.no_preload')
        ;

    $services->set(\Nsq\NsqBundle\EventListener\AckUnrecoverableMessageListener::class)
        ->tag('kernel.event_subscriber')
        ;
};
