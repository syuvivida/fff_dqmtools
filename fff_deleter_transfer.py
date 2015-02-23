import fff_deleter
import logging

def __run__(self, opts):
    log = logging.getLogger(__name__)

    ramdisk = "/fff/output/transfer/"
    tag = "fff_deleter_transfer"

    service = fff_deleter.FileDeleter(
        top = ramdisk,
        app_tag = tag,
        thresholds = {
            'rename': 60,
            'delete': 80,
        },
        log = log,
        report_directory = opts["path"],
        fake = opts["deleter.fake"],
    )
    service.delay_seconds = 15*60

    import gevent
    return (gevent.spawn(service.run_greenlet), service, )
