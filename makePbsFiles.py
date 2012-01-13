#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
Brief:   Python script to write all necessary parameters to a PBS script
         to run on Athena0 to execute the Image Simulator software.

Date:    June 03, 2010
Author:  Nicole Silvestri, U. Washington, nms21@uw.edu
Updated: June 07, 2011 - nms

Notes:   Modules here are called by fullFocalplanePbs.py.
         Requires imsimHomePath/pbs/distributeFiles.py.

"""

import sys, string, re, os
#import lsst.pex.policy as pexPolicy
import ConfigParser
import datetime
import time

def header(pbsfilename, nodedir, policy, sensorId, filter):
    """

    Write some typical PBS header file information.

    NOTES: The simulator jobs require only one node per job
    for PT1.2 (as of 5/3/11).  This may change in the future.

    """

    """
    saveDir          = policy.get('general','savedir')
    scratchpartition = policy.get('pbs','scratchpartition')
    """

    saveDir          = policy.get('general','saveDir')
    scratchpartition = policy.get('general','scratchPath')

    
    jobname          = policy.get('general','jobname')
    processors       = policy.getint('general','processors')
    nodes            = policy.getint('general','numNodes')
    pmem             = policy.getint('general','pmem')
    walltime         = policy.get('pbs','walltime')
    username         = policy.get('pbs','username')   
    rootEmail        = policy.get('pbs','rootEmail')
    queueTmp         = policy.get('pbs','queue')
    
    
    if queueTmp == 'astro':
        queue = '-l qos=astro'
    else:
        queue = '-q %s' %(queueTmp)
            
    tempHist, rNum, sNum, eNumber = nodedir.split('_')
    obshistid = re.sub('%s/' %(username),'', tempHist)
    eNum, filNum = eNumber.split('f')
    filename = obshistid + '_' + rNum + '_' + sNum + '_' + eNum

    tempDate = datetime.date.today()
    sDate = str(tempDate)
    year, mo, day = sDate.split('-')
    filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
    filt = filtmap[filter]
    paramdir = '%s-f%s' %(obshistid, filt)
    visitPath = os.path.join(saveDir, paramdir)
    
    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing header info for the PBS script" %(pbsfilename)
        sys.exit()
    
    print >>pbsout, "#!/bin/csh -x"
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### PBS script created by: %s on %s." %(username, sDate)
    print >>pbsout, "### For use with Imsim trunk version code."
    print >>pbsout, "### myJobId %s" %(sensorId)
    print >>pbsout, "### ---------------------------------------"
    #print >>pbsout, "#PBS -S /bin/tcsh"
    print >>pbsout, "#PBS -N %s"  %(jobname)
    # set email address for job notification
    print >>pbsout, "#PBS -M %s%s" %(username, rootEmail)
    print >>pbsout, "#PBS -m a"
    # Carry shell environment variables with the pbs job
    print >>pbsout, "#PBS -V"
    # Combine stdout and stderr in one stdout file
    print >>pbsout, "#PBS -j oe"
    print >>pbsout, "#PBS -o %s/logs/%s.out" %(visitPath, filename)
    print >>pbsout, "#PBS -l walltime=%s" %(walltime)
    print >>pbsout, "#PBS -l nodes=%s:ppn=%s" %(nodes, processors)
    print >>pbsout, "#PBS -l pmem=%sMB" %(pmem)
    print >>pbsout, "#PBS %s" %(queue)
    print >>pbsout, " "
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### Begin Imsim Executable Sections "
    print >>pbsout, "### ---------------------------------------"
    pbsout.close()
    return

def logging(pbsfilename, nodedir, policy):
    """

    Write some useful logging and diagnostic information to the
    logfiles.

    """
    
    scratchpartition = policy.get('general','scratchPath')

    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing logging info for PBS script" %(pbsfilename)
        sys.exit()
        
    print >>pbsout, " "
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### Logging information."
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "set pbs_job_id = `echo $PBS_JOBID | awk -F . '{print $1}'`"
    print >>pbsout, "set num_procs = `wc -l < $PBS_NODEFILE`"
    print >>pbsout, "set master_node_id = `hostname`"
    print >>pbsout, "echo This is job `echo $pbs_job_id`"
    print >>pbsout, "echo The master node of this job is `echo $master_node_id`"
    print >>pbsout, "echo The directory from which this job was submitted is `echo $PBS_O_WORKDIR`"
    if nodedir != None:
        print >>pbsout, "echo The directory in which this job will run is %s/%s" %(scratchpartition, nodedir)
    else:
        print >>pbsout, "echo No local node directory was indicated - job will run in `echo $PBS_O_WORKDIR`"
    print >>pbsout, "echo This job is running on `echo $num_procs` processors"
    print >>pbsout, "echo This job is starting at `date`"
    print >>pbsout, "echo ---"
    pbsout.close()
    return

#def setupCleanup(pbsfilename, nodedir, policy, sensorid, filter):
def setupCleanup(pbsfilename, nodedir, policy, sensorid):
    
    """

    Adapted from the example script on the UW CLuster Wiki. Employs
    Richard Coffey's cleanup_files.csh.  If the job terminates
    incorrectly, and you've copied files/directories to the node, this
    script does the cleanup for you.
    
    We ssh back to the head node as the compute nodes do not have
    access to the PBS command set.  Also there is no way to provide
    command line arguments to a script submitted to PBS, so we use a
    workaround by defining environment variables.

    """

    imsimHomePath    = os.getenv("IMSIM_HOME_DIR")
    useDb            = policy.get('general','useDatabase')
    catGenPath       = policy.get('lsst','catGen')
    scratchpartition = policy.get('general','scratchPath')
    username         = policy.get('pbs','username')
    
    scratchdir = '%s/%s' %(scratchpartition, nodedir)
    
    tempHist, rNum, sNum, eNumber = nodedir.split('_')
    obshistid = re.sub('%s/' %(username),'', tempHist)
    
    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing cleanup script info for PBS script" %(pbsfilename)
        sys.exit()
        
    print >>pbsout, " "
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### Set up the cleanup script."
    print >>pbsout, "### Mkdir and cd to your local node dir."
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, " "
    print >>pbsout, "set local_scratch_dir = %s" %(scratchdir)
    print >>pbsout, "set job_submission_dir = $PBS_O_WORKDIR"
    print >>pbsout, "set obshistid = %s" %(obshistid)
    print >>pbsout, "set cat_gen = %s" %(catGenPath)
    print >>pbsout, "set sensorid = %s" %(sensorid)
    print >>pbsout, "set username = %s" %(username)
   #print >>pbsout, "set filter = %s" %(filter) 
    if useDb == 1:
        print >>pbsout, "set minerva0_command = 'cd '$job_submission_dir'; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_error.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir',OBSHISTID='$obshistid',SENSORID='$sensorid',CAT_GEN='$cat_gen',USERNAME='$username" %(nodedir)
        #print >>pbsout, "set minerva0_command = 'cd '$job_submission_dir'; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_error.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir',OBSHISTID='$obshistid',SENSORID='$sensorid',CAT_GEN='$cat_gen',USERNAME='$username',FILTER='$filter" %(nodedir)
    else:    
        print >>pbsout, "set minerva0_command = 'cd '$job_submission_dir'; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_files.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir" %(nodedir)
    print >>pbsout, "echo $minerva0_command"
    print >>pbsout, "set pbs_output = `ssh minerva0 $minerva0_command`"
    print >>pbsout, "set cleanup_job_id = `echo $pbs_output | awk -F. '{print $1}'`"
    print >>pbsout, "echo I just submitted cleanup job ID $cleanup_job_id"
    print >>pbsout, "echo ---"
    print >>pbsout, " "
    # create local directories
    print >>pbsout, "## create local node directories (/%s/nodedir = %s)" %(scratchpartition, scratchdir)
    # check if directory already exists - remember you're writing to a csh script
    print >>pbsout, "if (! -d %s) then" %(scratchpartition)
    print >>pbsout, "  echo 'Are you sure you're on a node?'; exit 1"
    print >>pbsout, "endif"
    print >>pbsout, "if (! -d %s) then" %(scratchdir) # see if directory exists
    print >>pbsout, "  mkdir -p %s" %(scratchdir)  # make the directory (including parents)
    # note that this will overwrite previous files
    print >>pbsout, "endif"
    print >>pbsout, "if (! -d %s) then" %(scratchdir) # check if directory creation worked
    print >>pbsout, "  echo 'Something failed in creating local directory %s. Exiting.'" %(scratchdir)
    print >>pbsout, "  exit 1"
    print >>pbsout, "endif"
    print >>pbsout, "cd %s" %(scratchdir)
    pbsout.close()
    return
    
def writeJobCommand(pbsfilename, commandfile, nodedir):

    """
    
    Add the tcsh job commands from your commandfile.

    NOTE: At this point, you are in the /state/partition/nodedir.
    Your job should copy the necessary job files to here (likely
    already done in the commandfile).  Your job should ALSO copy the
    output back to the share disks (not here on the node) location.
    Use saveData() method below.
    
    """
    
    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing jobCommands for PBS script" %(pbsfilename)
        sys.exit()
        
    print >>pbsout, " "
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### Start your personal executable section"
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, " "
    
    try:
        jobinput = open(commandfile, 'r')
    except IOError:
        print "Could not open %s for reading tcsh script for PBS job" %(commandfile)
        sys.exit()
    if nodedir == None:
        print >>pbsout, "cd $PBS_O_WORKDIR" 
    for lines in jobinput:
        print >>pbsout, "%s" %(lines)
    jobinput.close()
    if nodedir == None:
        print >>pbsout, "echo ---"
        print >>pbsout, "echo PBS job finished at `date`"
    pbsout.close()
    return

def saveData(pbsfilename, nodedir, nodeDatadir, filter, policy):

    """

    Add the tcsh job commands to move the directories from the node to
    your shared disks on the cluster for normal script operation -
    nonfailure mode.  The cleanup script will remove all node
    directory data if abnormal exit occurs.  No data will be copied if
    cleanup script is invoked.

    """
    imsimHomePath = os.getenv("IMSIM_HOME_DIR")
    transferPath = policy.get('general','saveDir')
    scratchpartition = policy.get('general','scratchPath')
    username = policy.get('pbs','username')   
    datadir = policy.get('general','scratchDataDir')
 
    tempHist, rNum, sNum, eNumber = nodedir.split('_')
    obshistid = re.sub('%s/' %(username),'', tempHist)
    eNum, filNum = eNumber.split('f')
    filename = obshistid + '_' + rNum + '_' + sNum + '_' + eNum

    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing saveData commands for PBS script" %(pbsfilename)
        sys.exit()

    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### MOVE the image files to shared directory"
    print >>pbsout, "### ---------------------------------------"
    
    eimage = 'eimage_%s_f%s_%s_%s_%s.fits.gz' %(obshistid, filter, rNum, sNum, eNum)
    baseName = 'eimage'
    print >>pbsout, "echo scratchpartition: %s  nodedir: %s  nodeDataDir: %s" %(scratchpartition, nodedir, nodeDatadir)
    print >>pbsout, "echo Now moving %s/%s/%s/%s" %(scratchpartition, nodedir, nodeDatadir, eimage)
    print >>pbsout, "echo calling python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, eimage, baseName)
    print >>pbsout, "python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, eimage, baseName)
   
    axList = ['0','1']
    ayList = ['0','1','2','3','4','5','6','7']
    for ax in axList:
        for ay in ayList:
            image = 'imsim_%s_f%s_%s_%s_C' %(obshistid, filter, rNum, sNum) + ax+ay + '_%s.fits.gz' %(eNum)
            baseName = 'imsim'
            print >>pbsout, "echo Now moving %s/%s/%s/%s" %(scratchpartition, nodedir, nodeDatadir, image)
            print >>pbsout, "echo calling python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, image, baseName)
            print >>pbsout, "python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, image, baseName)
            
    print >>pbsout, "echo Moved %s files to %s/" %(obshistid, transferPath)
    pbsout.close()
    return

#def cleanNodeDir(pbsfilename, nodedir, policy, sensorid, filter):
def cleanNodeDir(pbsfilename, nodedir, policy, sensorid):
    """

    Be a good Cluster citizen...leave no trace.  Add the tcsh job
    commands to remove the directories from the node (for normal
    script operation - nonfailure mode).

    """

    useDb            = policy.get('general','useDatabase')
    catGenPath       = policy.get('lsst','catGen')
    scratchpartition = policy.get('general','scratchPath')
    username         = policy.get('pbs','username')
    
    tempHist, rNum, sNum, eNumber = nodedir.split('_')
    obshistid = re.sub('%s/' %(username),'', tempHist)
    
    try:
        pbsout = open(pbsfilename, 'a')
    except IOError:
        print "Could not open %s for writing cleanup commands for PBS script" %(pbsfilename)
        sys.exit()
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "### DELETE the local node directories and all files."
    print >>pbsout, "### Does not delete parent directories if created"
    print >>pbsout, "### ---------------------------------------"
    print >>pbsout, "echo Now deleting files in %s/%s" %(scratchpartition, nodedir)
    print >>pbsout, "/bin/rm -rf %s/%s" %(scratchpartition, nodedir)
    print >>pbsout, "echo ---"
    print >>pbsout, "cd $PBS_O_WORKDIR"
    if useDb == 1:
        try:
            #print >>pbsout, "  python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s finished %s %s" %(catGenPath, obshistid, sensorid, username, filter)
            print >>pbsout, "  python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s finished %s %s" %(catGenPath, obshistid, sensorid, username)
            print >>pbsout, "  echo Updated the jobAllocator database with key: FINISHED"
        except OSError:
            print OSError
    print >>pbsout, "echo PBS job finished at `date`"
    print >>pbsout, " "
    print >>pbsout, "###"
    pbsout.close()
    return

