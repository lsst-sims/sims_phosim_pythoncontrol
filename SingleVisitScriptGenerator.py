#!/usr/bin/python
###################!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
Brief:   Python script to generate a PBS file for each instance
         catalog (trimfile) - one pbs script per visit.  Upon submission, each
         of these scripts will be run individually on a cluster node to create
         the necessary *.pars, *.fits and *.pbs files for individual sensor
         jobs.

Usage:   python generateVisitPbs.py [options]
Options: fileName: Name of file containing trimfile list
         imsimPolicyFile: Name of your policy file
         extraidFile: Name of the extraidFile to include

Date:    November 30, 2011
Authors: Nicole Silvestri, U. Washington, nms@astro.washington.edu
         Jeffrey P. Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated: November 30, 2011 JPG: Removed dependency on LSST stack by swapping
                                LSST policy file with Python ConfigParser

Notes:   The script takes a list of trimfiles to run.
         The extraidFile is the name of an additional file used to change the
         default imsim parameters (eg. to turn clouds off, create centroid files, etc).

Methods: 1. header: write the PBS script header info for the run
         2. logging: write some useful info for the log files
         3. setupCleanup: setup and launch the cleanup script.
            The cleanup script will cleanup crashed jobs on the node.
         4. writeJobCommands: add code command lines to the PBS script
         5. cleanNodeDir: Copy/move all data to stagePath2 and delete node
            directory and all remaining data.
         6. makePbsScripts: This is the main module. It makes the
            individual instance catalog job PBS
            scripts and tars up data for the node job.  The PBS scripts
            and tar file are saved to the directory you run the script in.

To Do:   Remove directory structure dependence from all imsim code.
         Include verifyData tests for data dir integrity.

"""

from __future__ import with_statement
import os, re, sys
import random
import subprocess
import shutil
import glob

#import lsst.pex.policy as pexPolicy
import ConfigParser
import getpass   # for getting username
import datetime
from AbstractScriptGenerator import *


class SingleVisitScriptGenerator(AbstractScriptGenerator):
    """
    Generates a script for doing the preprocessing steps for a single obsHistID.
    This class generates a generic shell script.  It can be used as a superclass
    for PBS- or Exacycle-specific script generation.

    This class is designed so that it only needs to be instantiated once unless
    something in the .cfg file changes.  Each call to makeScript() sets all the
    variables change per visit.
    """
    def __init__(self, scriptInvocationPath, scriptOutList, policy, imsimConfigFile, extraIdFile, execFileTgzName):
        self.imsimHomePath = os.getenv("IMSIM_HOME_PATH")
        self.imsimDataPath = os.getenv("CAT_SHARE_DATA")

        self.imsimConfigFile = imsimConfigFile
        self.extraIdFile = extraIdFile
        self.execFileTgzName = execFileTgzName
        self.scriptInvocationPath = scriptInvocationPath
        self.scriptOutList = scriptOutList
        #self.scriptFileName = scriptFileName
        self.policy = policy
        #self.obsHistID = obsHistID
        # Map filter number to filter character
        self.filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
        # Get ImSim version
        versionFile = os.path.join(self.imsimHomePath, 'raytrace/version')
        for line in open(versionFile).readlines():
            if line.startswith('Revision:'):
                name, self.revision = line.split()

        #
        # Get [general] config file info
        #
        #policy   = pexPolicy.Policy.createPolicy(imsimPolicy)
        self.pythonExec = self.policy.get('general','python-exec')
        # Job params
        self.numNodes = self.policy.get('general','numNodes')
        self.processors = self.policy.get('general','processors')
        self.pmem = self.policy.get('general','pmem')
        self.jobName = self.policy.get('general','jobname')
        self.debugLevel = self.policy.getint('general','debuglevel')
        self.sleepMax = self.policy.getint('general','sleepmax')
        # Directories and filenames
        self.scratchPath = self.policy.get('general','scratchExecPath')
        self.scratchSharedPath = self.policy.get('general','scratchSharedPath')
        self.savePath  = self.policy.get('general','savePath')
        self.stagePath  = self.policy.get('general','stagePath1')
        self.stagePath2 = self.policy.get('general','stagePath2')
        self.tarball  = self.policy.get('general','dataTarball')
        # Job monitor database
        self.useDatabase = self.policy.getboolean('general','useDatabase')
        return

    def jobFileName(self, obshistid, filt):
        return '%s_f%s.csh' %(obshistid, filt)

    def makeScript(self, obsHistID, origObsHistID, trimfileName, trimfileBasename, trimfilePath,
                   filt, filter, visitDir, visitLogPath):
        """
        This creates a script to do the pre-processing for each visit.

        It calls 8 sub-methods in this class that each represent different phases of the job
        (* indicates these are defined in AbstractScriptGenerator):
           - writeHeader            Write script header
           - writeSetupExecDirs*    Write commands to setup the directories on exec node
           - writeCopySharedData*   Write commands to copy the shared data tarball to exec node
           - writeCopyStagedFiles   Write commands to copy staged data to exec node
           - writeJobCommands       Write the actual execution commands
           - writeCleanupCommands*  Write the commands to cleanup
           - tarVisitFiles          Tar the visit files that will be staged to the exec node
           - stageFiles             Stage files from submit node to exec node

        These can each be redefined in subclasses as needed

        To prevent conflicts between parallel workunits, the files needed for
        each work unit are packaged in scratchPath/visitDir where 'visitDir'
        is the directory within scratchPath that contains info for the particular
        objhistid + filter

        """
        scriptFileName = self.jobFileName(obsHistID, filt)
        # Get rid of any extraneous copies of the script file in the invocation
        # directory.  These should only be there if the script aborted.
        if os.path.isfile(scriptFileName):
            os.remove(scriptFileName)


        self.writeHeader(scriptFileName, visitDir, filter, obsHistID, visitLogPath)
        self.writeSetupExecDirs(scriptFileName, visitDir)
        self.writeCopySharedData(scriptFileName, visitDir)
        self.writeCopyStagedFiles(scriptFileName, trimfileName, trimfileBasename, trimfilePath,
                              filt, filter, obsHistID, origObsHistID, visitDir)
        self.writeJobCommands(scriptFileName, trimfileName, trimfileBasename, trimfilePath,
                              filt, filter, obsHistID, origObsHistID, visitDir)
        self.writeCleanupCommands(scriptFileName, visitDir)

        visitFileTgz = self.tarVisitFiles(obsHistID, filt)
        self.stageFiles(trimfileName, trimfileBasename, trimfilePath,
                        filt, filter, obsHistID, origObsHistID,
                        visitFileTgz, scriptFileName, self.scriptOutList, visitDir)


    def writeHeader(self, scriptFileName, visitDir, filter, obsHistID, visitLogPath):
        username = getpass.getuser()
        sDate = str(datetime.datetime.now())
        try:
            with file(scriptFileName, 'a') as cshOut:
                if self.debugLevel > 0:
                    print >>cshOut, "#!/bin/csh -x"
                else:
                    print >>cshOut, "#!/bin/csh"
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Shell script created by: %s " %(username)
                print >>cshOut, "###              created on: %s " %(sDate)
                print >>cshOut, "### Running SVN imsim revision %s." %(self.revision)
                print >>cshOut, "### workUnitID: %s" %(visitDir)
                print >>cshOut, "### obsHistID: %s" %(obsHistID)
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, " "
                cshOut.write('unalias cp \n')
                cshOut.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
                cshOut.write('setenv IMSIM_HOME_PATH %s \n' %(self.imsimHomePath))
        except IOError:
            print "Could not open %s for writing shell script" %(scriptFileName)
            sys.exit()
        return

    def writeCopyStagedFiles(self, scriptFileName, trimfileName, trimfileBasename, trimfilePath,
                             filt, filter, obshistid, origObshistid, visitDir):

        """
        Write the commands to copy staged files to the exec node

        """
        stagePath = self.stagePath
        visitPath = os.path.join(self.scratchPath, visitDir)

        try:
            with file(scriptFileName, 'a') as cshOut:

                print >>cshOut, " "
                print >>cshOut, "### -----------------------------------------"
                print >>cshOut, "### Copy files from stagePath1 to exec node"
                print >>cshOut, "### -----------------------------------------"
                print >>cshOut, " "

                cshOut.write('cd %s \n' %(visitPath))
                #
                # Copy trimfiles from staging
                #
                trimfileStagePath = os.path.join(stagePath, 'trimfiles', visitDir)
                # Now copy the entire directory in trimfileStagePath to the compute node
                cshOut.write('echo Copying contents of %s to %s.\n' %(trimfileStagePath, visitPath))
                cshOut.write('cp -a %s/* %s\n' %(trimfileStagePath, visitPath))

                #
                # Copy visitFiles from staging
                #
                cshOut.write('cp %s/visitFiles%s-f%s.tar.gz %s \n' %(stagePath, obshistid, filt, visitPath))
                #cshOut.write('gunzip visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                cshOut.write('tar xzvf visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                cshOut.write('rm visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                #
                # Copy execFiles from staging
                #
                cshOut.write('echo Copying and untarring %s to %s\n' %(self.execFileTgzName, visitPath))
                cshOut.write('cp %s . \n' %(os.path.join(stagePath, self.execFileTgzName)))
                cshOut.write('tar xzvf %s \n' %(self.execFileTgzName))
                cshOut.write('rm %s \n' %(self.execFileTgzName))
                # Set soft link to the catalog directory
                cshOut.write('echo Setting soft link to data directory. \n')
                cshOut.write('ln -s %s/ data \n' %(os.path.join(self.scratchSharedPath, 'data')))
                # scratchOutputPath gets made in fullFocalPlane
                #cshOut.write('mkdir %s \n' %(self.scratchOutputPath))

        except IOError:
            print "Could not open %s for writing jobCommands for PBS script" %(self.scriptFileName)
            sys.exit()

        ## close file
        #pbsout.close()
        return

    def writeJobCommands(self, scriptFileName, trimfileName, trimfileBasename, trimfilePath,
                         filt, filter, obshistid, origObshistid, visitDir):

        """
        Add the actual job commands.
        You should also copy the list of PBS files back to stagePath2.

        """
        visitPath = os.path.join(self.scratchPath, visitDir)
        try:
            with file(scriptFileName, 'a') as cshOut:

                print >>cshOut, " "
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Executable section"
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, " "
                cshOut.write('cd %s \n' %(visitPath))
                cshOut.write('echo Running fullFocalplane.py with %s. \n' %(self.extraIdFile))
                cshOut.write('which %s\n' %(self.pythonExec))
                cshOut.write("%s fullFocalplane.py %s %s %s '' '' '' '' ''\n"
                             %(self.pythonExec, trimfileBasename, self.imsimConfigFile, self.extraIdFile))
                cshOut.write('cp %s_f%sJobs.lis %s \n'%(obshistid, filt, self.stagePath2))

                #for lines in jobinput:
                #    print >>pbsout, "%s" %(lines)
                #jobinput.close()
        except IOError:
            print "Could not open %s for writing jobCommands for PBS script" %(scriptFileName)
            sys.exit()
        return

    def tarVisitFiles(self, obshistid, filt):
        """
        Make tarball of the control and param files needed on the exec nodes.
        """

        # We should be in the script's invocation directory
        assert os.getcwd() == self.scriptInvocationPath

        visitFileTar = 'visitFiles%s-f%s.tar.gz' %(obshistid, filt)
        #visitFileGzip = '%s.gz' %(visitFileTar)
        cmd = 'tar czvf %s chip.py fullFocalplane.py AbstractScriptGenerator.py AllChipsScriptGenerator.py SingleChipScriptGenerator.py %s %s' %(visitFileTar, self.imsimConfigFile, self.extraIdFile)

        print 'Tarring control and param files that will be copied to the execution node(s).'
        subprocess.check_call(cmd, shell=True)

        return visitFileTar



    def stageFiles(self, trimfileAbsName, trimfileBasename, trimfilePath,
                   filt, filter, obshistid, origObshistid,
                   visitFileTgz, scriptFileName, scriptOutList, visitDir):
        stagePath = self.stagePath
        # We should be in the script's invocation directory
        assert os.getcwd() == self.scriptInvocationPath
        # Move the visit and script files to stagedir.
        print 'Moving Exec, Visit & Script Files to %s:' %(stagePath)
        # Use shutil.copy instead of shutil.move because the former overwrites
        shutil.copy(self.execFileTgzName, stagePath)
        os.remove(self.execFileTgzName)
        shutil.copy(visitFileTgz, stagePath)
        os.remove(visitFileTgz)
        shutil.copy(scriptFileName, stagePath)
        os.remove(scriptFileName)
        os.chmod(os.path.join(stagePath,scriptFileName), 0775)


        # Also stage the trimfiles to stagePath/visitDir/trimfiles/
        trimfileStagePath = os.path.join(stagePath, 'trimfiles', visitDir)
        print 'Staging trimfile %s to %s:' %(trimfileAbsName, trimfileStagePath)
        if not os.path.isdir(trimfileStagePath):
            print 'Making trimfile stage path: %s' %(trimfileStagePath)
            os.mkdir(trimfileStagePath)
        shutil.copy(trimfileAbsName, trimfileStagePath)
        # Make sure that there is at least one "includeobj" line in trimfile
        cmd = ('grep includeobj %s' %(trimfileAbsName))
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        nincobj = len(results)
        # If so, stage the files in the pops directory for this object
        if nincobj > 0:
            popsPath = os.path.join(trimfilePath, 'pops') # Orig location for 'pops' dir
            dest = '%s/pops' %(trimfileStagePath)
            os.mkdir(dest)  # Create dest dir
            # Copy the file glob with origObshistid
            for singleFile in glob.glob('%s/*%s*' %(popsPath, origObshistid)):
                print '   Moving %s to %s/pops' %(singleFile, dest)
                shutil.copy(singleFile, dest)

        # Generate the list of job scripts for the ray tracing and post processing
        fileDest = os.path.join(stagePath, scriptFileName)
        print 'Attempting to add %s to file %s in %s' %(fileDest, scriptOutList,
                                                        self.scriptInvocationPath)
        #os.chdir(self.imsimHomePath)  Now created in scriptInvocationPath
        with file(scriptOutList, 'a') as parFile:
            parFile.write('%s \n' %(fileDest))



class SingleVisitScriptGenerator_Pbs(SingleVisitScriptGenerator):

    def __init__(self, scriptInvocationPath, scriptOutList, policy, imsimConfigFile,
                 extraIdFile, execFileTgzName):
        """
        Augment the superclass's constructor to have the PBS 'username' appended to it.
        """
        SingleVisitScriptGenerator.__init__(self, scriptInvocationPath, scriptOutList, policy,
                                             imsimConfigFile, extraIdFile, execFileTgzName)
        username = self.policy.get('pbs','username')
        #self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), username)
        return

    def jobFileName(self, obshistid, filt):
        return '%s_f%s.pbs' %(obshistid, filt)

    def writeHeader(self, scriptFileName, visitDir, filter, obshistid, visitLogPath):

        """
        Write PBS-specific header information.
        This also calls self.logging() and self.setupCleanup()

        """
        policy = self.policy
        #obshistid = self.obsHistID

        saveDir = self.savePath
        processors = self.processors
        nodes = self.numNodes
        pmem = self.pmem
        jobname = self.jobName
        walltime         = policy.get('pbs','walltime')
        username         = policy.get('pbs','username')
        rootEmail        = policy.get('pbs','rootEmail')
        queueTmp         = policy.get('pbs','queue')

        if queueTmp == 'astro':
            queue = '-l qos=astro'
        else:
            queue = '-q %s' %(queueTmp)


        filt = self.filtmap[filter]

        filename = '%s_f%s' %(obshistid, filt)
        paramdir = '%s-f%s' %(obshistid, filt)
        savePath = os.path.join(saveDir, paramdir)

        try:
            with file(scriptFileName, 'a') as pbsOut:
                if self.debugLevel > 0:
                    print >>pbsOut, "#!/bin/csh -x"
                else:
                    print >>pbsOut, "#!/bin/csh"
                print >>pbsOut, "### ---------------------------------------"
                print >>pbsOut, "### PBS script created by: %s " %(username)
                print >>pbsOut, "### Running SVN imsim revision %s." %(self.revision)
                print >>pbsOut, "### ---------------------------------------"
                #print >>pbsOut, "#PBS -S /bin/csh -x"
                print >>pbsOut, "#PBS -N %s"  %(jobname)
                # set email address for job notification
                print >>pbsOut, "#PBS -M %s%s" %(username, rootEmail)
                print >>pbsOut, "#PBS -m a"
                # Carry shell environment variables with the pbs job
                print >>pbsOut, "#PBS -V"
                # Combine stdout and stderr in one stdout file
                print >>pbsOut, "#PBS -j oe"
                print >>pbsOut, "#PBS -o %s/%s.out" %(visitLogPath, filename)
                print >>pbsOut, "#PBS -l walltime=%s" %(walltime)
                print >>pbsOut, "#PBS -l nodes=%s:ppn=%s" %(nodes, processors)
                print >>pbsOut, "#PBS -l pmem=%sMB" %(pmem)
                print >>pbsOut, "#PBS %s" %(queue)
                print >>pbsOut, " "
                pbsOut.write('unalias cp \n')
                pbsOut.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
                pbsOut.write('setenv IMSIM_HOME_PATH %s \n' %(self.imsimHomePath))
                pbsOut.write('echo Setting up the LSST Stack to get proper version of Python. \n')
                pbsOut.write('source /share/apps/lsst_gcc440/loadLSST.csh \n')
                #pbsOut.write('echo Setting up pex_logging, _exceptions, and _policy packages. \n')
                #pbsOut.write('setup pex_policy \n')
                #pbsOut.write('setup pex_exceptions \n')
                #pbsOut.write('setup pex_logging \n')
        except IOError:
            print "Could not open %s for writing header info for the PBS script" %(scriptFileName)
            sys.exit()

        self.logging(scriptFileName, visitDir)
        self.setupCleanup(scriptFileName, visitDir)
        return

    def logging(self, scriptFileName, visitDir):

        """

        Have the script write some useful logging and diagnostic information to the
        logfiles.

        """
        pbsfilename = scriptFileName
        visitPath = os.path.join(self.scratchPath, visitDir)


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
        if visitDir != None:
            print >>pbsout, "echo The directory in which this job will run is %s" %(visitPath)
        else:
            #print >>pbsout, "echo No local node directory was indicated - job will run in `echo $PBS_O_WORKDIR`"
            print 'ERROR: You must specify a remote execution directory!'
            quit()
        print >>pbsout, "echo This job is running on `echo $num_procs` processors"
        print >>pbsout, "echo This job is starting at `date`"
        print >>pbsout, "echo ---"
        # close file
        pbsout.close()
        return

    def setupCleanup(self, scriptFileName, visitDir):

        """

        Adapted from the example script on the cluster wiki. Employs Jeff
        Gardner's cleanup_files.csh.  If the job terminates incorrectly,
        and you've copied files/directories to the node, this script
        does the cleanup for you.

        We ssh back to the head node as the compute nodes do not have
        access to the PBS command set.  Also there is no way to provide
        command line arguments to a script submitted to PBS, so we use a
        workaround by defining environment variables.

        """
        pbsfilename = scriptFileName
        policy = self.policy

        visitPath = os.path.join(self.scratchPath, visitDir)

        try:
            pbsout = open(pbsfilename, 'a')
        except IOError:
            print "Could not open %s for writing cleanup script info for PBS script" %(pbsfilename)
            sys.exit()

        print >>pbsout, " "
        print >>pbsout, "### ---------------------------------------"
        print >>pbsout, "### Set up the cleanup script."
        print >>pbsout, "### ---------------------------------------"
        print >>pbsout, " "
        print >>pbsout, "set local_scratch_dir = %s" %(visitPath)
        print >>pbsout, "set job_submission_dir = $PBS_O_WORKDIR"
        print >>pbsout, "set minerva0_command = 'cd %s; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_files.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir" %(self.imsimHomePath,visitDir)
        print >>pbsout, "echo $minerva0_command"
        print >>pbsout, "#set pbs_output = `ssh minerva0 $minerva0_command`"
        print >>pbsout, "#set cleanup_job_id = `echo $pbs_output | awk -F. '{print $1}'`"
        print >>pbsout, "#echo I just submitted cleanup job ID $cleanup_job_id"
        print >>pbsout, "echo ---"
        print >>pbsout, " "
        # close file
        pbsout.close()
        return
