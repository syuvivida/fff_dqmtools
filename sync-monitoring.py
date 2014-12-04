import sys
import subprocess
import os

hosts = [
    "bu-c2f13-31-01",
    "fu-c2f13-39-01",
    "fu-c2f13-39-03",
    "fu-c2f13-39-04",

    # playback
    "fu-c2f13-41-03",
    "fu-c2f13-41-01",
]

install_dir = os.path.join(os.path.dirname(__file__), "./")
plugin_dir = os.path.join(install_dir, "dqm.esplugin/")
destination_dir = "/usr/share/elasticsearch/plugins/dqm/"

def rsync(host, source, destination, delete=False):
    if delete:
        subprocess.call(["rsync", "-avzc", "--delete", "--rsync-path=sudo rsync", source, host + ":" + destination])
    else:
        subprocess.call(["rsync", "-avzc", "--rsync-path=sudo rsync", source, host + ":" + destination])

def sync_site(host):
    rsync(host, plugin_dir, destination_dir, delete=True)

def install_daemon(host):
    rsync(host, install_dir + "fff_monitoring.py", "/usr/local/bin/")
    rsync(host, install_dir + "fff_monitoring", "/etc/init.d/")
    rsync(host, install_dir + "fff_dqmtools.logrotate", "/etc/logrotate.d/fff_dqmtools")

    subprocess.call(["ssh", host, "sudo", "chown", "root:root",
        "/usr/local/bin/fff_monitoring.py",
        "/etc/init.d/fff_monitoring",
        "/etc/logrotate.d/fff_dqmtools",
    ])

    subprocess.call(["ssh", host, "sudo", "chmod", "644",
        "/etc/logrotate.d/fff_dqmtools",
    ])

    subprocess.call(["ssh", host, "sudo", "chmod", "755",
        "/usr/local/bin/fff_monitoring.py",
        "/etc/init.d/fff_monitoring",
    ])

    subprocess.call(["ssh", host, "sudo", "/sbin/chkconfig", "fff_monitoring", "on"])
    subprocess.call(["ssh", host, "sudo", "/etc/init.d/fff_monitoring", "restart"])

if len(sys.argv) > 1:
    hosts = sys.argv[1:]

for host in hosts:
    print "installing:", host

    sync_site(host)
    install_daemon(host)
