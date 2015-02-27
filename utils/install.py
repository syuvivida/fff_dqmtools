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
    call(["sudo yum -y remove fff-dqmtools"], shell=True)
    call(["sudo /etc/init.d/fff_dqmtools stop"], shell=True)
    call(["sudo /etc/init.d/fff_deleter stop"], shell=True)
    call(["sudo /etc/init.d/fff_deleter_minidaq stop"], shell=True)
    call(["sudo /etc/init.d/fff_deleter_transfer stop"], shell=True)
    call(["sudo rm -f /etc/init.d/fff_deleter"], shell=True)
    call(["sudo rm -f /etc/init.d/fff_deleter_minidaq"], shell=True)
    call(["sudo rm -f /etc/init.d/fff_deleter_transfer"], shell=True)
    call(["sudo rm -frv /opt/fff_dqmtools"], shell=True)
    call(["sudo", "yum", "-y", "install", rpm])


    #call(["sudo", "/etc/init.d/fff_monitoring", "stop"])
    #call(["sudo", "/etc/init.d/fff_monitoring", "stop"])
    #call(["sudo", "chkconfig", "--del", "fff_monitoring"])
    #call(["sudo", "rm", "/etc/init.d/fff_monitoring"])
    #call(["sudo", "rm", "/var/run/fff_monitoring.pid"])
    #call(["sudo ls -la /var/run/fff_*"], shell=True)
    #call(["sudo ps aux | grep fff_"], shell=True)
    #call(["ps aux | grep fff_dqm"], shell=True)
    #call(["sudo", "yum", "-y", "reinstall", rpm])
    #call(["sudo", "chkconfig", "fff_dqmtools", "on"])

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

    rpms = glob.glob("../tmp/RPMBUILD/RPMS/x86_64/*.rpm")
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
        if len(sys.argv) > 2:
            hosts = sys.argv[2:]
        else:
            import fff_cluster
            for k, hosts in fff_cluster.get_node()["_all"].items():
                print k + ":", " ".join(hosts)

            sys.exit(1)

        for host in hosts:
            install_remote(spath, host)
    else:
        print "Please provide either --local or --remote"


