# this should later become a reader for a configuration file in /etc/

import socket

clusters = {
    'production_c2f11': ["bu-c2f11-09-01.cms", "fu-c2f11-11-01.cms", "fu-c2f11-11-02.cms", "fu-c2f11-11-03.cms", "fu-c2f11-11-04.cms", ],
    'playback_c2f11': ["bu-c2f11-13-01.cms", "fu-c2f11-15-01.cms", "fu-c2f11-15-02.cms", "fu-c2f11-15-03.cms", "fu-c2f11-15-04.cms", ],
    'lookarea_c2f11': ["bu-c2f11-19-01.cms", ]
}

def get_host():
    host = socket.gethostname()
    host = host.lower()

    return host

def get_node():
    host = get_host()

    current = {
        "_all": clusters,
    }

    for key, lst in clusters.items():
        if host in lst:
            current["node"] = host
            current["nodes"] = lst
            current["label"] = key
            break

    return current

def host_wrapper(allow = []):
    """ This is decorator for function.
        Runs a function of the given hosts,
        just returns on others.
    """

    host = get_host()

    def run_wrapper(f):
        return f

    def noop_wrapper(f):
        def noop(*args, **kwargs):
            name = kwargs["name"]
            log = kwargs["logger"]

            log.info("The %s applet is not allowed to run on %s, disabling", name, host)
            return None

        return noop

    if host in allow:
        return run_wrapper
    else:
        return noop_wrapper

if __name__ == "__main__":
    print get_node()
