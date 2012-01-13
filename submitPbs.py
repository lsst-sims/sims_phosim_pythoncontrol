#!/opt/rocks/bin/python

"""

Brief:   Python script to submit and manage time between qsubs to
         individual nodes.  Updates job database of selected.

Date:    May 03, 2010
Author:  Nicole Silvestri, U. Washington, nms21@uw.edu
Updated: June 07, 2011 - nms

Usage:   python submitPbs.py fileName policy 
Options: fileName: a list of files containg pbs scripts to launch
         policy: your copy of the imsimPolicyPbs.paf file

Notes: * file should contain the names of the output lists from
         the jobs created by running generateVisitPbs.py.  It is then,
         a list of lists.

       * You should copy imsimPbsPolicy.paf to a different file and
         modify it to the appropriate settings for your account before
         running this script.

"""
from __future__ import with_statement
import os, re, sys
import time, datetime
import subprocess
import lsst.pex.policy as pexPolicy

def submit(file, policy):

    """

    The code submits one file at a time with appropriate wait times
    between job (CCD) submissions so as not to flood the scheduler's
    queue.  A job monitor database is updated upon each submission and
    at the beginning of each job.  
    
    """

    # Read the PBS files from your file list
    myFiles = '%s' %(infile)
    files = open(myFiles).readlines()

    # Get necessary policy file info
    workDir = os.getcwd()
    imsimPolicy = os.path.join(workDir, policy)
    myPolicy = pexPolicy.Policy.createPolicy(imsimPolicy)
    sleep = myPolicy.getInt('sleep') 
    wait = myPolicy.getInt('wait')
    username = myPolicy.getString('username')
    nJobsMax = myPolicy.getInt('maxJobs')
    catGen = myPolicy.getString('catGen')
    useDb = myPolicy.getInt('useDatabase')
    submittedFileList = 'submittedFiles.lis'
    
    for pbs in files:
        pbs = pbs.strip()
        print 'Submitting %s Sensor Jobs to Cluster.' %(pbs)
        submitCcds(pbs, username, sleep, wait, nJobsMax, catGen, useDb, files, submittedFileList)
    return

def submitCcds(pbs, username, sleep, wait, nJobsMax, catGen, useDb, files, submittedFileList):
    pbsFiles = open(pbs).readlines()
    x = 1
    y = 0
    while len(pbsFiles):
        print 'Total Number of PBS Files:', len(pbsFiles)
        #cmd = 'qstat | grep %s' %(username)
	cmd = "showq | grep %s | grep Running" %(username)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        #nJobs = len(results)
        nJobsRunning = len(results)       
        cmd = "showq | grep %s | grep Idle" %(username)	
	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        nJobsIdle = len(results)
        #nToSubmit = nJobsMax - nJobs
        nToSubmit = nJobsMax - (nJobsRunning + nJobsIdle)
        print 'Max Number to submit: ', nToSubmit
        done = 'False'
        jobMonitor = 'python/lsst/sims/catalogs/generation/jobAllocator' 
        catgenDir = os.path.join(catGen, jobMonitor)        
        
        for i in range(nToSubmit):

            try:
                myfile = pbsFiles.pop(0)
                myfile = myfile.strip()
                pfile = os.path.basename(myfile)
                if pfile.startswith('pbs_'):
                    #pf, obshistid, filt, raft, sensor, snapext = pfile.split('_')
                    #filter = filt.strip('f')
                    pf, obshistid, raft, sensor, snapext = pfile.split('_')

                if pfile.startswith('8'):
                    obshistid, filterext = pfile.split('_')
                    useDb = 2

                print 'Obshistid:', obshistid
                sensorId = '%s_genJob_%i' %(obshistid, x)
                for line in open(myfile).readlines(): 
                    if line.startswith('### myJobId'):
                        pd, name, sensorId = line.split()
                        
                print 'sensorId:', sensorId
                
                cmd  = 'qsub %s' %(myfile)
                subprocess.check_call(cmd, shell=True)
                print '%i: Submitted PBS File: %s' %(x, myfile)
                
                with file(submittedFileList, 'a') as subFile:
                    subFile.write('%s \n' %(myfile))

                if useDb == 1:
                    #cmd = 'python %s/myJobTracker.py %s qsubbed %s %s' %(catgenDir, obshistid, sensorId, username, filter)
                    cmd = 'python %s/myJobTracker.py %s qsubbed %s %s' %(catgenDir, obshistid, sensorId, username)
                    try:
                        print 'Updating jobTracker.'
                        subprocess.check_call(cmd, shell=True)
                    except OSError:
                        print OSError
                        print 'Job Monitor Database NOT Updated.'
                        pass
                else:
                    print 'Not Updating Job Monitor Database.'
                    
                x += 1
                print 'Waiting for %s seconds to submit next job.' %(wait)
                print ''
                time.sleep(wait)
                done = 'False'         
            except:
                # out of files!
                print 'Finished submitting all requested PBS jobs in list %s.' %(pbs)
                done = 'True'
                y += 1
                break
        if done == 'True':
            break
        else:
            print 'Waiting for some jobs to clear out of the queue.'
            print 'Sleeping for %s seconds.' %(sleep)
            time.sleep(sleep)

    now = datetime.datetime.now()
    print 'Finished submitting all jobs in list %s on %s: ' %(pbs, now.ctime())
        
    return

if __name__ == "__main__":

    if not len(sys.argv) == 3:
        print "usage: python submitPbs.py fileName imsimPolicyFile"
        quit()

    infile = sys.argv[1]
    policy = sys.argv[2]
    
    submit(infile, policy)

