#!/usr/bin/env python

import argparse
import sys
import os
import time
import datetime
import tempfile
import commands
from jira.client import JIRA

def get_jira_config():
  # read the config file
  home=jira_home=os.getenv('HOME')
  home=home.rstrip('/')
  jira_config = dict(line.strip().split('=') for line in open(home + '/jira.ini'))
  return jira_config

def get_jira():
  options = {
    'server': 'https://issues.apache.org/jira'
  }

  jira_config = get_jira_config()
  jira = JIRA(options=options,basic_auth=(jira_config['user'], jira_config['password']))
  return jira

def cmd_exists(cmd):
  status, result = commands.getstatusoutput(cmd)
  return status

def main():
  ''' main(), shut up, pylint '''
  popt = argparse.ArgumentParser(description='Kafka patch review tool')
  popt.add_argument('-b', '--branch', action='store', dest='branch', required=True, help='Tracking branch to create diff against')
  popt.add_argument('-j', '--jira', action='store', dest='jira', required=True, help='JIRA corresponding to the reviewboard')
  popt.add_argument('-s', '--summary', action='store', dest='summary', required=False, help='Summary for the reviewboard')
  popt.add_argument('-d', '--description', action='store', dest='description', required=False, help='Description for reviewboard')
  popt.add_argument('-r', '--rb', action='store', dest='reviewboard', required=False, help='Review board that needs to be updated')
  popt.add_argument('-t', '--testing-done', action='store', dest='testing', required=False, help='Text for the Testing Done section of the reviewboard')
  popt.add_argument('-db', '--debug', action='store_true', required=False, help='Enable debug mode')
  opt = popt.parse_args()

  post_review_tool = None
  if (cmd_exists("post-review") == 0):
    post_review_tool = "post-review"
  elif (cmd_exists("rbt") == 0):
    post_review_tool = "rbt post"
  else:
    print "please install RBTools"
    sys.exit(1)

  patch_file=tempfile.gettempdir() + "/" + opt.jira + ".patch"
  if opt.reviewboard:
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
    patch_file=tempfile.gettempdir() + "/" + opt.jira + '_' + st + '.patch'

  # first check if rebase is needed
  git_branch_hash="git rev-parse " + opt.branch
  p_now=os.popen(git_branch_hash)
  branch_now=p_now.read()
  p_now.close()

  git_common_ancestor="git merge-base " + opt.branch + " HEAD"
  p_then=os.popen(git_common_ancestor)
  branch_then=p_then.read()
  p_then.close()

  if branch_now != branch_then:
    print 'ERROR: Your current working branch is from an older version of ' + opt.branch + '. Please rebase first by using git pull --rebase'
    sys.exit(1)

  git_configure_reviewboard="git config reviewboard.url https://reviews.apache.org"
  print "Configuring reviewboard url to https://reviews.apache.org"
  p=os.popen(git_configure_reviewboard)
  p.close()

  git_remote_update="git remote update"
  print "Updating your remote branches to pull the latest changes"
  p=os.popen(git_remote_update)
  p.close()

  rb_command= post_review_tool + " --publish --tracking-branch " + opt.branch + " --target-groups=kafka --bugs-closed=" + opt.jira
  if opt.debug:
    rb_command=rb_command + " --debug"
  summary="Patch for " + opt.jira
  if opt.summary:
    summary=opt.summary
  rb_command=rb_command + " --summary \"" + summary + "\""
  if opt.description:
    rb_command=rb_command + " --description \"" + opt.description + "\""
  if opt.reviewboard:
    rb_command=rb_command + " -r " + opt.reviewboard
  if opt.testing:
    rb_command=rb_command + " --testing-done=" + opt.testing
  if opt.debug:
    print rb_command
  p=os.popen(rb_command)
  rb_url=""
  for line in p:
    print line
    if line.startswith('http'):
      rb_url = line
    elif line.startswith("There don't seem to be any diffs"):
      print 'ERROR: Your reviewboard was not created/updated since there was no diff to upload. The reasons that can cause this issue are 1) Your diff is not checked into your local branch. Please check in the diff to the local branch and retry 2) You are not specifying the local branch name as part of the --branch option. Please specify the remote branch name obtained from git branch -r'
      p.close()
      sys.exit(1)
    elif line.startswith("Your review request still exists, but the diff is not attached") and not opt.debug:
      print 'ERROR: Your reviewboard was not created/updated. Please run the script with the --debug option to troubleshoot the problem'
      p.close()
      sys.exit(1)
  if p.close() != None:
    print 'ERROR: reviewboard update failed. Exiting.'
    sys.exit(1)
  if opt.debug:
    print 'rb url=',rb_url

  git_command="git format-patch " + opt.branch + " --stdout > " + patch_file
  if opt.debug:
    print git_command
  p=os.popen(git_command)
  p.close()

  print 'Creating diff against', opt.branch, 'and uploading patch to JIRA',opt.jira
  jira=get_jira()
  issue = jira.issue(opt.jira)
  attachment=open(patch_file)
  jira.add_attachment(issue,attachment)
  attachment.close()

  comment="Created reviewboard "
  if not opt.reviewboard:
    print 'Created a new reviewboard',rb_url,
  else:
    print 'Updated reviewboard',rb_url
    comment="Updated reviewboard "

  comment = comment + rb_url + ' against branch ' + opt.branch
  jira.add_comment(opt.jira, comment)

  #update the JIRA status to PATCH AVAILABLE
  transitions = jira.transitions(issue)
  transitionsMap ={}
  
  for t in transitions:
    transitionsMap[t['name']] = t['id']

  jira_config = get_jira_config()

  if('Submit Patch' in transitionsMap):
     jira.transition_issue(issue, transitionsMap['Submit Patch'] , assignee={'name': jira_config['user']} )


if __name__ == '__main__':
  sys.exit(main())
