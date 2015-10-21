import subprocess
import logging
import fnmatch
import pickle
import time
import json
import sys
import os
import re

from collections import namedtuple

log_prefix = int(os.environ.get("CMSSW_MANAGER_LOG_PREFIX", "0"))
logging.basicConfig(level=logging.INFO, format="[%d] " % log_prefix+ '%(message)s')

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

def log_raw(msg):
    sys.stderr.write(msg)
    sys.stderr.flush()

Ref = namedtuple('Ref', ['hash', 'type', 'object'])
PullRequest = namedtuple('PullRequest', ['id', 'merge', 'head'])
MergeRequest = namedtuple('MergeRequest', ['id', 'type', 'label', 'arg'])
ScramProject = namedtuple('ScramProject', ['project', 'title', 'tag', 'arch', 'path', 'mtime'])
Commit = namedtuple('Commit', ['hash', 'title'])

CacheFile = "~/.cmssw_deploy.pkl"
UserConfigFile = "~/.cmssw_deploy.jsn"

def shell_cmd(cmd, callback=None, guard=False, **kwargs):
    log.info("Exec: %s", repr(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, **kwargs)

    for line in p.stdout:
        line = line.strip()

        r = False
        if callback:
            r = callback(line)

        if r is False or r is None:
            log.info("o: %s", line)
        elif r is True:
            pass
        else:
            log.info("o: %s", r)

    ret = p.wait()

    if ret != 0:
        log.warning("Return code: %s", ret)

        if guard:
            raise Exception("Command failed with return code: %s" % ret)

    return ret

def check_if_hash(x):
    if len(x) < 5:
        return False

    try:
        int(x.lower(), base=16)
        return True
    except:
        return False

class UserCache(object):
    """ Manager global git/scram information,
        as well as configuration parameters.
    """

    def __init__(self):
        self.git_references = []
        self.git_pr_dict = {}
        self.scram_projects = []

        self.scan_time = None 

    def save(self):
        fp = os.path.expanduser(CacheFile)
        with open(fp, "w") as f:
            pickle.dump(self, f)

        log.info("Save pull request and release info to: %s", fp)

    @classmethod
    def load(cls):
        obj = cls()
        try:
            fp = os.path.expanduser(CacheFile)
            with open(fp, "r") as f:
                o = pickle.load(f)
                obj.git_references = o.git_references
                obj.git_pr_dict = o.git_pr_dict
                obj.scram_projects = o.scram_projects
                obj.scan_time = o.scan_time

        except:
            log.warning("Failed to open user cache.", exc_info=True)
        
        log.info("Loaded %d tags and %d pull requests", len(obj.git_references), len(obj.git_pr_dict))
        log.info("Loaded %d scram releases", len(obj.scram_projects))

        return obj

    def update(self, config):
        self.update_git_stuff(config)
        self.update_scram_stuff(config)

        self.scan_time = time.time()


    def update_git_stuff(self, config):
        refs = []
        pr_dict = {}

        def parse_ref(line):
            s = line.split()
            if len(s) < 2: return False

            hsh = s[0]
            obj = s[1]

            if not check_if_hash(hsh): return False

            # not interested in the head
            if obj == "HEAD": return True

            s = obj.split("/")
            if len(s) < 3: return False

            if s[1] == "tags":
                refs.append(Ref(hash=hsh, type="tag", object=obj))
            elif s[1] == "heads":
                refs.append(Ref(hash=hsh, type="head", object=obj))
            elif s[1] == "signatures":
                pass
            elif s[1] == "pull":
                i = int(s[2])
                pr = pr_dict.get(i, PullRequest(id=i,merge=None,head=None))

                if s[3] == "head":
                    pr = pr._replace(head=hsh)
                elif s[3] == "merge":
                    pr = pr._replace(merge=hsh)
                else:
                    return False

                pr_dict[i] = pr
            else:
                return False

            return True

        repo = config.get_key('git_repository')
        log.info("Updating git info at: %s", repo)
        shell_cmd(["git", "ls-remote", repo], callback=parse_ref)

        log.info("Found %d tags and %d pull requests", len(refs), len(pr_dict))
        self.git_references = refs
        self.git_pr_dict = pr_dict
        
    def update_scram_stuff(self, config):
        arch_re = re.compile(r"slc\d_amd64_gcc\d\d\d")

        projects = []
        def parse_scram(line):
            s = line.split()
            if len(s) != 3: return False

            project, tag, path = s
            arch = None

            m = arch_re.search(path) 
            if m: arch = m.group(0)

            try:
                mtime = os.stat(path).st_mtime
            except:
                log.warning("Failed to get mtime for path: %s", path)
                mtime = None

            p = ScramProject(project=project, title=tag, tag=tag, arch=arch, path=path, mtime=mtime)
            projects.append(p)
            return True

        log.info("Updating scram info")
        shell_cmd(["scram", "l", "--all", "-c", "CMSSW"], callback=parse_scram)
        log.info("Found %d scram releases", len(projects))
        self.scram_projects = projects

def select_target(available, tag, arch, tag_blacklist):
    import readline
    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims(" ")

    # we need to find one with multiple architectures
    # and display them with different titles (even though they are the same)
    tag_counts = {}
    for x in available:
        tag_counts[x.tag] = tag_counts.get(x.tag, 0) + 1

    anew = []
    for x in available:
        if tag_counts[x.tag] > 1:
            anew.append(x._replace(title=x.tag + ":" + x.arch))
        else:
            anew.append(x)

    anew.sort(key=lambda x: x.mtime, reverse=True)
    available = anew

    while True:
        # check for an exact match, special case
        x = filter(lambda x: tag == x.title, available)
        if len(x) == 1: return x[0]

        filtered = list(filter(lambda x: tag in x.title, available))

        completer_lst = []
        def completer_func(text, state):
            if state == 0:
                completer_lst[:] = filter(lambda x: x.title.startswith(text), filtered)

            if state < len(completer_lst):
                return completer_lst[state].title
            else:
                return None

        # print the choices
        if tag: log.warning("Filter: *%s*" % tag)
        readline.set_completer(completer_func)
        line = raw_input('Tag to use (tab to complete, %d entries): ' % len(filtered))
        readline.set_completer(None)
        line = line.strip()
        tag = line

def get_list_of_pr(string):
    # validate pull requests
    pull_requests = []
    if string:
        for p in string.split(","):
            i, t = int(p), "merge-topic"
            if p.startswith("+"):
                t = "cherry-pick"
                
            m = MergeRequest(i, t, label="+%d" % i, arg=p)
            pull_requests.append(m)

    return pull_requests

def parse_commits(diff):
    commits = []
    def parse_commit(line):
        line = line.strip()
        h = line.partition(" ")
        c = Commit(hash=h[0], title=h[2])
        commits.append(c)

        return True

    shell_cmd(["git", "log", "--pretty=oneline", diff], callback=parse_commit)
    return commits


def get_commits(uc, mr):
    pr = uc.git_pr_dict[mr.id]
    return parse_commits("HEAD..%s" % pr.head), 'HEAD', pr.head

def get_commits_vs_base(uc, mr):
    pr = uc.git_pr_dict[mr.id]

    parents = []
    def parse_commit(line):
        x = line.strip().split()
        if x: parents[:] = x 
        return True
    shell_cmd(["git", "show", "--pretty=%P", "%s" % pr.merge], callback=parse_commit)

    parents = set(parents)
    parents.remove(pr.head)

    if len(parents) != 1:
        raise Exception("Too many parents.")

    parent = parents.pop()
    return parse_commits("%s..%s" % (parent, pr.head)), parent, pr.head

def compare_commit_lists(list1, list2):
    list1 = set(list1)
    list2 = set(list2)

    d1, d2 = list1.difference(list2), list2.difference(list1)
    for d in d1:
        log.warning("Diff :-: %s", d)

    for d in d2:
        log.warning("Diff :+: %s", d)

    return (len(d1) + len(d2)) == 0

def apply_actual_pr(uc, mr, args):
    # fetch the pr refs
    shell_cmd(["git", "fetch", "official-cmssw", "refs/pull/%s/*:refs/gh-remotes/pull/%s/*" % (mr.id, mr.id, )], guard=True)

    # check if this pr "compatible" with the branch we are on
    # basically, this checks if commits which would be applied during merging are 
    # the same as the ones seen in github
    log.info("Merging: %s", mr)
    c1 = get_commits(uc, mr)
    c2 = get_commits_vs_base(uc, mr)
    r = compare_commit_lists(c1[0], c2[0])

    if mr.type == "merge-topic":
        # do the sparse checkout magic
        shell_cmd(["git", "cms-sparse-checkout", c1[1], c1[2]], guard=True)
        shell_cmd(["git", "read-tree", "-mu", "HEAD"], guard=True)

        log.info("'%s' is a merge-topic, will be done via git cms-merge-topic." % mr.id)
        shell_cmd(["git", "cms-merge-topic", str(mr.id)], guard=True)
    elif mr.type == "cherry-pick":
       raise Exception("Not yet implemented")

    log.info("Merge successful: %s", mr)

def apply_pr(uc, args):
    # some verification checks:
    list_os_mr = get_list_of_pr(args.pull_requests)
    if len(list_os_mr) != 1:
        raise Exception("Only one pull request can be applied using apply-pr.")

    mr = list_os_mr[0]

    # we have to be in src and "git" command is in available
    if not os.environ.has_key("CMSSW_BASE"):
        raise Exception("Please do cmsenv before calling me.")

    base_path = os.environ["CMSSW_BASE"]
    base_src_path = os.path.join(base_path, "src")
    common_prefix = os.path.commonprefix([os.getcwd(), base_src_path])

    if common_prefix != base_src_path:
        raise Exception("You have to be inside project's src directory.")

    # check for staged changes, abort if any (for safety)
    failed = []
    def fail_on_change(x):
        x = x.strip()
        if len(x) and x[0] in "ACDMRU":
            failed.append(x)
            log.error("Staged change: %s", x)
        return True

    shell_cmd(["git", "status", "--porcelain", "--untracked=no"], callback=fail_on_change)

    if failed:
        raise Exception("Staged, but not commited change was found, aborting.")

    backup_src_path = None
    if not args.no_backup:
        for i in range(1, 1024):
            b = os.path.join(base_path, "src.pre" + mr.label + ".%d" % i)
            if not os.path.exists(b):
                backup_src_path = b
                break

    if backup_src_path:
        log.info("Making a backup at: %s", backup_src_path)
        r = shell_cmd(["rsync", "-ap", base_src_path + "/", backup_src_path + "/", ], guard=True)
        if r == 0:
            backup_reverse_cmd = ["rsync", "-ap", "--delete", backup_src_path + "/", base_src_path + "/", ]
            backup_reverse_cmd2 = ["rm", "-fr", backup_src_path]
            log.info("Backup successful: %s", backup_src_path)
            log.info("Do this to restore: \"%s\"", " ".join(backup_reverse_cmd))

    try:
        if mr.type in ("merge-topic", "cherry-pick"):
            apply_actual_pr(uc, mr, args)
            pass
    except:
        log.error("Merge of %s has failed", mr, exc_info=True)

        if backup_src_path and not args.no_restore:
            log.info("Restoring old directory")
            shell_cmd(backup_reverse_cmd, guard=True)
            shell_cmd(backup_reverse_cmd2, guard=True)
            log.info("Done, it's probably a good idea to do \"cd; cd -\"")

        raise

def apply_multiple_pr(uc, args, cmd_prefix=[], cwd=None):
    """ wrapper to apply multiple pr, it will launch subprocesses for each pr """

    list_os_mr = get_list_of_pr(args.pull_requests)
    status = {}

    script = os.path.abspath(sys.argv[0])

    for mr in list_os_mr:
        argv = cmd_prefix + [sys.executable, script, ]
        if args.no_backup: argv.append("--no-backup")
        if args.no_restore: argv.append("--no-restore")
        argv += ["apply-pr", "-p", mr.arg]

        env = os.environ.copy()
        env["CMSSW_MANAGER_LOG_PREFIX"] = str(log_prefix + 1)

        r = shell_cmd(argv, env=env, cwd=cwd, stderr=subprocess.STDOUT)
        status[mr] = r

        if r != 0 and args.no_restore:
            return

    log.warning("Applied merge requests (%d):", len(list_os_mr))
    for mr in list_os_mr:
        if status[mr] == 0:
            log.info("%s: success", mr)
        else:
            log.info("%s: failed", mr)

def make_release(uc, args):
    # find the release to use
    target = select_target(uc.scram_projects, args.tag, args.arch, args.tag_blacklist)
    log.info("Selected release: %s %s", target.arch, target.tag)

    # generate the directory_name
    components = [target.tag]
    if args.label:
        components = [args.label, "_",] + components

    pull_requests = get_list_of_pr(args.pull_requests)
    for m in pull_requests:
        components.append(m.label)

    name = "".join(components)
    log.info("Generated directory name: %s", name)

    # check if directory exists
    if os.path.exists(name):
        log.error("Directory path (%s) already exists, delete it.", name)
        sys.exit(1)

    # create a scram project there
    if os.environ.get("SCRAM_ARCH", "x") != target.arch:
        log.info("Setting SCRAM_ARCH=%s", target.arch)
        os.environ["SCRAM_ARCH"] = target.arch

    shell_cmd(["scram", "p", "-n", name, target.tag])

    # create a special "wrapper" file, to help us execute commands
    # inside the cmssw environment
    with open(os.path.join(name, "cmswrapper.sh"), "w") as f:
        os.fchmod(f.fileno(), 0755)
        f.write("#!/bin/sh\n")
        f.write("")
        f.write("eval `scramv1 runtime -sh`\n")
        f.write("")
        f.write("exec \"$@\"\n")
        
    # do git init
    log.info("Doing git init (this takes time)")
    base_src_path = os.path.join(name, "src")
    shell_cmd(["./cmswrapper.sh", "git-cms-init", "--ssh"], cwd=name)
    shell_cmd(["../cmswrapper.sh", "git", "tag", "cmssw_manager_base"], cwd=base_src_path)

    if args.pull_requests:
        log.info("Applying pr string: %s", args.pull_requests)
        cmd_prefix = ["../cmswrapper.sh"] 
        apply_multiple_pr(uc, args, cmd_prefix=cmd_prefix, cwd=base_src_path)

    log.warning("Made release area: %s", name)

class UserConfig(object):
    """ Manager user configuration parameters.
    """

    def __init__(self):
        self.fp = os.path.expanduser(UserConfigFile)
        self.store = {}

    def save(self):
        with open(self.fp, "w") as f:
            json.dump(self.store, f, indent=2)

    def load(self):
        try:
            with open(self.fp, "r") as f:
                self.store = json.load(f)
        except:
            log.warning("Failed to open user config.", exc_info=True)


    def get_config(self, key, default=None):
        return self.store.get(key, default)

    def update_config(self, key, value):
        if self.store.get(key) == value:
            return

        log.info("Setting key %s to: %s", key, value)
        self.store[key] = value

    def get_key(self, key):
        r = self.store.get(key, None)
        if r is None:
            raise Exception("Key %s not found in the config." % key)
        return r


def parse_args(user_config):
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("command", help="What to do: update,make-release,select-release,apply-pr,apply-multiple-pr,build")

    group = parser.add_argument_group('global_info', 'Global information')
    group.add_argument("-g", "--repo", type=str, default=user_config.get_config("git_repository", "git@github.com:cms-sw/cmssw.git"), help="Main git repository.")
    group.add_argument("-u", action="store_true", help="Force update/rescan of github and scram, for new releases and stuff.")
    group.add_argument("--no-save", action="store_true", help="Don't save config/github/scram cache.")

    group = parser.add_argument_group('release_info', 'Release information')
    group.add_argument("-t", "--tag", type=str, default="", help="Main release tag to use as a base (this is scram tag, not git's.")
    group.add_argument("-b", "--tag-blacklist", type=str, default="ROOT5,THREADED,ICC,CLANG,DEVEL", help="Blacklist words in scram tags (they don't appear in auto-select).")
    group.add_argument("-a", "--arch", type=str, default="", help="Scram arch, don't set for auto-select.")
    group.add_argument("-p", "--pull-requests", type=str, default="", help="Comma-separate list of pull requests to apply. Use \"+number\" to merge using cherry pick instead of merge-topic.")
    group.add_argument("-l", "--label", type=str, default="", help="Label to prefix the directory with.")

    group = parser.add_argument_group('merge_info', 'Merging options')
    group.add_argument("--no-backup", action="store_true", help="Don't make backup of the src dir before merging.")
    group.add_argument("--no-restore", action="store_true", help="Restore from backup in case of a merge failure.")

    args = parser.parse_args()
    user_config.update_config("git_repository", args.repo)

    return args

if __name__ == "__main__":
    user_config = UserConfig()
    user_config.load()

    args = parse_args(user_config)

    # update config_file 
    if not args.no_save:
        user_config.save()

    # init or load git/scram cache
    if args.u:
        user_cache = UserCache()
        needs_update = True
    else:
        user_cache = UserCache.load()
        t = time.time() - (user_cache.scan_time or 0)
        needs_update = (t < 0) or (t >= 60*60)

    if needs_update:
        user_cache.update(user_config)
        if not args.no_save:
            user_cache.save()

    command = args.command
    if command == "update":
        pass
    elif command == "select-release":
        target = select_target(user_cache.scram_projects, args.tag, args.arch, args.tag_blacklist)
        print target
    elif command == "make-release":
        make_release(user_cache, args)
    elif command == "apply-pr":
        apply_pr(user_cache, args)
    elif command == "apply-multiple-pr":
        apply_multiple_pr(user_cache, args)
    else:
        log.error("Unknown command: %s", command)
