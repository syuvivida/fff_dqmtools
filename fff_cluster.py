# this should later become a reader for a configuration file in /etc/

clusters = {
    'production': ["bu-c2f13-31-01", "fu-c2f13-39-02", "fu-c2f13-39-01", "fu-c2f13-39-03", "fu-c2f13-39-04"],
    'playback': [ "bu-c2f13-29-01", "fu-c2f13-41-01", "fu-c2f13-41-03"],
}

def get_node():
    import socket
    host = socket.gethostname()
    host = host.lower()

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

if __name__ == "__main__":
    print get_node()
