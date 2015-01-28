import sys
import subprocess
import os
import glob
import subprocess

def call(*kargs, **kwargs):
    print "Running:", kargs
    r = subprocess.call(*kargs, **kwargs)
    print "Finished command:", r

def install_local(rpm):
    call(["sudo", "rm", "-vf", "/usr/local/bin/fff_monitoring.py"])
    call(["sudo", "rm", "-vf", "/usr/local/bin/fff_monitoring.pyc"])
    call(["sudo", "yum", "-y", "install", rpm])
    call(["sudo", "ps", "auxf"])

def install_remote(spath, host):
    print "*"*80
    print "* Installing on:", host
    for a in range(7):
        print "*"
    print "*"*80

    subprocess.call(["ssh", host, "python", spath, "--local"])

if __name__ == "__main__":
    # cd to the current directory
    spath = os.path.abspath(__file__)
    os.chdir(os.path.dirname(spath))

    rpms = glob.glob("./tmp/RPMBUILD/RPMS/x86_64/*.rpm")
    rpm = None
    if len(rpms) == 1:
        rpm = rpms[0]
        print "RPM:", rpm
    else:
        print "RPM not found, do ./makerpm.sh"
        sys.exit(1)


    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        sys.exit(install_local(rpm))
    elif len(sys.argv) > 1 and sys.argv[1] == "--remote":
        import fff_cluster
        x = fff_cluster.get_node()
        for label, hosts in x["_all"].items():
            for host in hosts:
                install_remote(spath, host)
    else:
        print "Please provide either --local or --remote"


