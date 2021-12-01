#!/usr/bin/env python2

# this should later become a reader for a configuration file in /etc/

import socket
import subprocess
import os
from threading import Timer

clusters = {
  'production_c2f11': ["bu-c2f11-09-01.cms", "fu-c2f11-11-01.cms", "fu-c2f11-11-02.cms", "fu-c2f11-11-03.cms", "fu-c2f11-11-04.cms", ],
  'playback_c2f11': ["bu-c2f11-13-01.cms", "fu-c2f11-15-01.cms", "fu-c2f11-15-02.cms", "fu-c2f11-15-03.cms", "fu-c2f11-15-04.cms", ],
  'lookarea_c2f11': ["bu-c2f11-19-01.cms", ]
}

def popen_timeout(seconds=10, cmd):
  kill = lambda process: process.kill()
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  timer = Timer(seconds, kill, [p])
  answer = " ... "
  try:
    timer.start()
    answer, stderr = p.communicate()
  except Exception as error_log:
    answer = error_log
  timer.cancel()
  return answer

def get_hltd_version(host):
  return popen_timeout(5, ["ssh " + host + " \"rpm -qf /opt/hltd\""])
  
def get_hltd_version_all():
  answer = {}
  for key, lst in clusters.items():
    subanswer = []
    if host in lst:
      hltd_version = get_hltd_version( host )
      subanswer += [ [ host, hltd_version ] ]
    answer[key] = subanswer

  return subanswer

def get_simulator_config(opts, simulator_host):
  if not simulator_host : return ""
  path = self.opts["simulator.conf"]
  cfg = popen_timeout(5, ["ssh " + simulator_host + " \"cat " + path + "\""])
  return cfg

def update_config(cfg, key, value):
  if not key : return cfg
  if not key in cfg : return cfg
  cfg[ key ] = value
  return cfg

def write_config(opts, simulator_host, cfg): # FIXME
  if not simulator_host : return
  answer = popen_timeout(5, ["ssh " + simulator_host + " \"cat " + path + "\""])
  return answer

def get_simulator_runs(opts, simulator_host):
  if not simulator_host : return []
  cfg_json = get_simulator_config(opts, simulator_host)
  cfg = json.loads( cfg_json )
  path = os.path.dirname( cfg["source"] )
  runs_raw = popen_timeout(5, ["ssh " + simulator_host + " \"ls -1d " + path + "/run*\""])
  runs = []
  for run in runs_raw.split("\n"):
    runs += [ os.path.basename( run ) ]
  return runs

def restart_hltd(host):
  return popen_timeout(15, ["ssh " + host + " \"sudo -i /sbin/service hltd stop; sudo -i /sbin/service hltd start\""])

def restart_fff(host):
  return popen_timeout(15, ["ssh " + host + " \"sudo systemctl restart fff_dqmtools.service\""])

def get_txt_file(host, path, timeout=30):
  if not path : return ""
  return popen_timeout(timeout, ["ssh " + host + " \"cat " + path + "\""])

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
