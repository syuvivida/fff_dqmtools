fff_dqmtools
============

fff_dqmtools aka DQM^2

#### Installation
Create a RPM locally executing `makerpm.sh`. Then:

* for the manual installation at P5 do something `./install.py --remote machine_name` with RPM stored in the same dir. This installation will be reverted by puppet months or days later.  
* to update playback DQM machines the dropbox could be used https://twiki.cern.ch/twiki/bin/view/CMS/ClusterUsersGuide#How_to_use_the_dropbox_computer:  
```
ssh cmsdropbox.cms
sudo dropbox2 -o cc7 -z cms -s dqm -u folder_with_fff_rpm/
```

* to update production machines please create a JIRA ticket.
