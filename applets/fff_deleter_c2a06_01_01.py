import fff_dqmtools
import fff_cluster
import applets.fff_deleter as fff_deleter
import logging

@fff_cluster.host_wrapper(allow = ["dqmrubu-c2a06-01-01"])
@fff_dqmtools.fork_wrapper(__name__)
@fff_dqmtools.lock_wrapper
def __run__(opts, **kwargs):
    log = kwargs["logger"]

    ramdisk = "/fff/ramdisk/"
    tag = "fff_deleter_c2a06_01_01"

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
    service.delay_seconds = 30

    service.run_greenlet()

