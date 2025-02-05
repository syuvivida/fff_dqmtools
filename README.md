fff_dqmtools
============

fff_dqmtools aka DQM^2
branch for migration to new DQM RUBU machines

#### Installation
Create a RPM locally executing `makerpm.sh`. Then:

* for the manual installation at P5 do something `./install.py --remote machine_name` with RPM stored in the same dir. This installation will be reverted by puppet months or days later.  
* to update playback DQM machines the dropbox could be used https://twiki.cern.ch/twiki/bin/view/CMS/ClusterUsersGuide#How_to_use_the_dropbox_computer:  
```
ssh cmsdropbox.cms
sudo dropbox2 -o cc7 -z cms -s dqm -u folder_with_fff_rpm/
```

* to update production machines please create a JIRA ticket.

#### DQM^2 worflows:
##### to update DQM^2 DB with client info:
Create JSON report files in:
https://github.com/cms-DQM/fff_dqmtools/blob/a62a1a317e1bc312c704065a43d3bd0aeb5ecc74/applets/analyze_files.py#L100
Then read JSONs in folder:
https://github.com/cms-DQM/fff_dqmtools/blob/a62a1a317e1bc312c704065a43d3bd0aeb5ecc74/applets/fff_filemonitor.py#L181
And upload with http:
https://github.com/cms-DQM/fff_dqmtools/blob/a62a1a317e1bc312c704065a43d3bd0aeb5ecc74/applets/fff_filemonitor.py#L82
To be catched with web client:
https://github.com/cms-DQM/fff_dqmtools/blob/a62a1a317e1bc312c704065a43d3bd0aeb5ecc74/applets/fff_web.py#L462
And saved to DB:
https://github.com/cms-DQM/fff_dqmtools/blob/a62a1a317e1bc312c704065a43d3bd0aeb5ecc74/applets/fff_web.py#L148
