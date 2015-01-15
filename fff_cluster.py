clusters = {
    'production': ["bu-c2f13-31-01", "fu-c2f13-39-02", "fu-c2f13-39-01", "fu-c2f13-39-03", "fu-c2f13-39-04"],
    'playback': [ "bu-c2f13-29-01", "fu-c2f13-41-01", "fu-c2f13-41-03"],
}

def get_cluster():
    import socket
    host = socket.gethostname()
    host = host.lower()

    for key, lst in clusters.items():
        if host in lst:
            return (key, lst, )

    return (None, None, )

if __name__ == "__main__":
    print get_cluster()
