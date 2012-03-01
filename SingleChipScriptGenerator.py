#!/usr/bin/python
#############!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
Brief:   Generates the script for doing the raytracing and postprocessing
         steps for a single CCD detector.

Date:    January 26, 2012
Authors: Nicole Silvestri, U. Washington, nms21@uw.edu
         Jeffrey P. Gardner, U. Washington, Google, gardnerj@phys.washington.edu

Notes:   Modules here are called by fullFocalplanePbs.py.
         Requires imsimSourcePath/pbs/distributeFiles.py.
"""

from __future__ import with_statement
import sys, string, re, os
#import lsst.pex.policy as pexPolicy
import ConfigParser
import datetime
import time
import random
import getpass   # for getting username
from AbstractScriptGenerator import *


class SingleChipScriptGenerator(AbstractScriptGenerator):
    """
    Generates the script for doing the raytracing and postprocessing steps
    for a single CCD chip.

    This class is designed so that it only needs to be instantiated once per
    LSST visit (i.e. once per call to fullFocalPlane.py).  The makeScripts()
    method can then be called successively to generate the script for each chip
    in the visit.

    """
    def __init__(self, policy, obshistid, filter, filt, centid, centroidPath,
                 stagePath2, paramDir, trackingParFile):
        """
        NTS: For PBS derived class, refine scratchPath as follows:

        """

        self.policy = policy
        self.obshistid = obshistid
        self.filter = filter
        self.filt = filt
        self.centid = centid
        self.centroidPath = centroidPath
        self.stagePath2 = stagePath2
        self.paramDir = paramDir
        self.trackingParFile = trackingParFile

        self.pythonExec = self.policy.get('general','python-exec')
        # Shared data locations
        #self.imsimDataPath = os.getenv("CAT_SHARE_DATA")
        self.imsimDataPath = self.policy.get('general','dataPathSEDs')
        self.useSharedData = self.policy.getboolean('general','useSharedSEDs')
        self.tarball = self.policy.get('general','dataTarballSEDs')
        if self.useSharedData == True:
          self.scratchSharedPath = self.policy.get('general','scratchDataPath')
        else:
          self.scratchSharedPath = os.path.join(self.imsimDataPath,'sharedData')
        # writeCopySharedData() will check the existence of self.dataCheckDir
        # to determine if it needs to grab and untar self.tarball.
        self.dataCheckDir = 'data/focal_plane/sta_misalignments/qe_maps'
        # Directories and filenames
        self.savePath  = self.policy.get('general','savePath')
        self.scratchPath = self.policy.get('general','scratchExecPath')
        self.scratchOutputDir = self.policy.get('general','scratchOutputDir')
        self.debugLevel = self.policy.getint('general','debuglevel')
        self.sleepMax = self.policy.getint('general','sleepmax')
        # Job monitor database
        self.useDb = self.policy.getboolean('general','useDatabase')
        return

    def dbSetup(self, cmdFile, sensorId):
        print 'Database not implemented in shell script version!'
        sys.exit()

    def dbCleanup(self, jobOut, obshistid, sensorId):
        print 'Database not implemented in shell script version!'
        sys.exit()


    def jobFileName(self,id):
        return 'exec_%s_%s.csh' %(self.obshistid, id)

    def makeScript(self, cid, id, rx, ry, sx, sy, ex, raytraceParFile, backgroundParFile, cosmicParFile,
                   sensorId, visitLogPath):
        """
        Make the script for the job for this CCD.

        In general, this method will be called 189 times per focal plane.
        189 script files per focalplane, 378 per trim file (2 snaps) if stars
        are present on every sensor.

        Each script will sleep for a random time between 0-'sleepmax' seconds
        to avoid locks being set at the same time on local nodes. Using
        the 'lockfile' command ensures that only 1 job per node executes
        the following if-then-else code block at a time.  The '-l 1800' is
        a safety catch where the lockfile will automatically be deleted
        after 30 minutes.

        This method calls 7 sub-methods that each represent different phases of the job
        (* indicates these are defined in AbstractScriptGenerator):
           - writeHeader              Write script header
           - writeSetupExecDirs*      Write commands to setup the directories on exec node
           - writeCopySharedData*     Write commands to copy the shared data tarball to exec node
           - writeCopyStagedFiles     Write commands to copy staged data to exec node
           - writeJobCommands         Write the actual execution commands
           - writeSaveOutputCommands  Write commands to save output images
           - writeCleanupCommands*    Write the commands to cleanup

        These can each be redefined in scheduler-specific subclasses as needed

        To prevent conflicts between parallel workunits, the files needed for
        each work unit are packaged in scratchPath/wuID where 'wuID' is the
        work unit ID and is constructed as:
               wuID = '%s_%sf%s' %(self.obshistid, id, self.filter)
        where 'id' is of the form:
               id = 'R'+rx+ry+'_'+'S'+sx+sy+'_'+'E00'+ex

        """

        # Make the PBS files, one for each CCD

        ##pbsFileName = 'pbs_%s_f%s_%s.pbs' %(self.obshistid, self.filter, id)
        #pbsFileName = 'pbs_%s_%s.pbs' %(self.obshistid, id)
        wuID = '%s_%sf%s' %(self.obshistid, id, self.filter)
        jobFileName = self.jobFileName(id)
        #nodedir = '%s/%s' %(self.username, ccdId)
        # self.scratchPath already has self.username in it.
        # Replace "nodedir" instances with "wuID"
        # wuPath = os.path.join(self.scratchPath, wuID)

        self.writeHeader(jobFileName, wuID, sensorId, visitLogPath)
        self.writeSetupExecDirs(jobFileName, wuID)
        self.writeCopySharedData(jobFileName, wuID)
        self.writeCopyStagedFiles(jobFileName, wuID, cid, id, raytraceParFile,
                                 backgroundParFile, cosmicParFile, sensorId)
        self.writeJobCommands(jobFileName, wuID, cid, id, rx, ry, sx, sy, ex)
        self.writeSaveOutputCommands(jobFileName, wuID, sensorId)
        self.writeCleanupCommands(jobFileName, wuID, sensorId)

        print "Created Job file %s" %(jobFileName)
        return


    def writeHeader(self, jobFileName, wuID, sensorId, visitLogPath):

        username = getpass.getuser()
        sDate = str(datetime.datetime.now())

        if os.path.isfile(jobFileName):
            os.remove(jobFileName)
        try:
            with file(jobFileName, 'a') as jobFile:
                if self.debugLevel > 0:
                    print >>jobFile, "#!/bin/csh -x"
                else:
                    print >>jobFile, "#!/bin/csh"
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, "### Shell script created by: %s " %(username)
                print >>jobFile, "###              created on: %s " %(sDate)
                print >>jobFile, "### workUnitID: %s" %(wuID)
                print >>jobFile, "### obsHistID:  %s" %(self.obshistid)
                print >>jobFile, "### sensorID:   %s" %(sensorId)
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, " "
                jobFile.write('unalias cp \n')
                #jobFile.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
        except IOError:
            print "Could not open %s to write header info in writeSetupCommands()" %(jobFileName)
            sys.exit()

    def writeCopyStagedFiles(self, jobFileName, wuID, cid, id, raytraceParFile,
                             backgroundParFile, cosmicParFile, sensorId):

        """
        Write the commands to copy staged files to the exec node

        """
        wuPath = os.path.join(self.scratchPath, wuID)

        try:
            with file(jobFileName, 'a') as jobFile:
                print >>jobFile, " "
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, "### Copy files from stagePath2 to exec node"
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, " "

                #
                # Update the jobAllocator database
                #
                if self.useDb == True:
                    self.dbSetup(jobFile, sensorId)
                else:
                    jobFile.write('echo JobDatabase Not Updated.  Not using database. \n')

                #
                # Copy files needed for the specific run (in nodefiles*.tar.gz)
                #
                jobFile.write('cp %s/nodeFiles%s.tar.gz %s/ \n' %(self.stagePath2, self.obshistid, wuPath))
                # cd to the scratch exec dir (where all of the exec and param files are stored
                # for this work unit).
                jobFile.write('cd %s \n' %(wuPath))
                jobFile.write('tar xzf nodeFiles%s.tar.gz \n' %(self.obshistid))
                jobFile.write('rm nodeFiles%s.tar.gz \n' %(self.obshistid))
                #
                # Set the soft link to the catalog directory
                #
                jobFile.write('echo Setting soft link to data directory. \n')
                jobFile.write('ln -s %s/ data \n' %(os.path.join(self.scratchSharedPath, 'sharedData')))
                #
                # Create the scratch output directory
                #
                jobFile.write('if (! -d %s) then \n' %(self.scratchOutputDir))
                jobFile.write('   echo "Creating %s." \n' %(self.scratchOutputDir))
                jobFile.write('   mkdir %s \n' %(self.scratchOutputDir))
                jobFile.write('endif \n')
                # Copy Files needed for LSST, Background, Cosmic Rays, & E2ADC executables
                #jobFile.write('cd $PBS_O_WORKDIR \n')
                jobFile.write('echo Copying files needed for LSST stage. \n')
                jobFile.write('cp %s/trimcatalog_%s_%s.pars %s/%s %s/%s %s/ \n' %(self.paramDir, self.obshistid, id, self.paramDir, raytraceParFile, self.paramDir, self.trackingParFile, wuPath))
                jobFile.write('echo Copying file needed for BACKGROUND stage. \n')
                jobFile.write('cp %s/%s %s/ \n' %(self.paramDir, backgroundParFile, wuPath))
                jobFile.write('echo Copying file needed for COSMIC RAY stage. \n')
                jobFile.write('cp %s/%s %s/ \n' %(self.paramDir, cosmicParFile, wuPath))
                jobFile.write('echo Copying files needed for E2ADC stage. \n')
                jobFile.write('cp %s/e2adc_%s_%s_*.pars %s/ \n' %(self.paramDir, self.obshistid, cid, wuPath))

        except IOError:
            print "Could not open %s for writing script in writejobCommands" %(jobFileName)
            sys.exit()
        return




    def writeJobCommands(self, jobFileName, wuID, cid, id, rx, ry, sx, sy, ex):

        """
        Add the commands to the script that actually do the work.

        """
        wuPath = os.path.join(self.scratchPath, wuID)

        try:
            with file(jobFileName, 'a') as jobFile:
                print >>jobFile, " "
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, "### Executable section"
                print >>jobFile, "### ---------------------------------------"
                print >>jobFile, " "
                jobFile.write('cd %s \n' %(wuPath))
                jobFile.write('echo Running chip.py %s %s %s %s %s %s %s %s \n' %(self.obshistid, self.filt, rx, ry, sx, sy, ex, self.scratchOutputDir))
                jobFile.write('%s chip.py %s %s %s %s %s %s %s %s \n' %(self.pythonExec, self.obshistid, self.filt, rx, ry, sx, sy, ex, self.scratchOutputDir))
                if self.centid == '1':
                    jobFile.write('echo Copying centroid file to %s \n' %(self.centroidPath))
                    jobFile.write('cp raytrace/centroid_imsim_%s_%s.txt %s \n' %(self.obshistid, id, self.centroidPath))

        except IOError:
            print "Could not open %s for writing script in writejobCommands" %(jobFileName)
            sys.exit()
        return


    def writeSaveOutputCommands(self, jobFileName, wuID, sensorId):

        """

        Add the commands to move the output data from the execution node
        to shared storage.

        In the PBS implementation, the cleanup script will remove all node
        directory data if abnormal exit occurs.  No data will be copied if
        cleanup script is invoked.

        """

        # Absolute path to output files on exec node = /"scratchPath"/wuID/"scratchOutputDir"
        scratchOutputPath = os.path.join(self.scratchPath, wuID, self.scratchOutputDir)

        tempHist, rNum, sNum, eNumber = wuID.split('_')
        #obshistid = re.sub('%s/' %(self.username),'', tempHist)
        eNum, filNum = eNumber.split('f')
        filename = self.obshistid + '_' + rNum + '_' + sNum + '_' + eNum

        try:
            jobOut = open(jobFileName, 'a')
        except IOError:
            print "Could not open %s for writing in writeSaveOutputCommands()" %(jobFileName)
            sys.exit()

        print >>jobOut, "### ---------------------------------------"
        print >>jobOut, "### MOVE the image files to shared directory"
        print >>jobOut, "### ---------------------------------------"

        eimage = 'eimage_%s_f%s_%s_%s_%s.fits.gz' %(self.obshistid, self.filt, rNum, sNum, eNum)
        baseName = 'eimage'
        print >>jobOut, "echo scratchPath: %s  wuID: %s  scratchOutputDir: %s" %(self.scratchPath, wuID, self.scratchOutputDir)
        print >>jobOut, "echo Now moving %s/%s" %(scratchOutputPath, eimage)
        #print >>jobOut, "echo calling python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, eimage, baseName)
        #print >>jobOut, "python pbs/distributeFiles.py %s %s/%s/%s/%s %s" %(transferPath, scratchpartition, nodedir, nodeDatadir, eimage, baseName)
        print >>jobOut, "echo calling %s pbs/distributeFiles.py %s %s/%s %s" %(self.pythonExec, self.savePath,
                                                                               scratchOutputPath, eimage, baseName)
        print >>jobOut, "%s pbs/distributeFiles.py %s %s/%s %s" %(self.pythonExec, self.savePath,
                                                                  scratchOutputPath, eimage, baseName)

        axList = ['0','1']
        ayList = ['0','1','2','3','4','5','6','7']
        for ax in axList:
            for ay in ayList:
                image = 'imsim_%s_f%s_%s_%s_C' %(self.obshistid, self.filt, rNum, sNum) + ax+ay + '_%s.fits.gz' %(eNum)
                baseName = 'imsim'
                print >>jobOut, "echo Now moving %s/%s" %(scratchOutputPath, image)
                print >>jobOut, "echo calling %s pbs/distributeFiles.py %s %s/%s %s" %(self.pythonExec, self.savePath, scratchOutputPath, image, baseName)
                print >>jobOut, "%s pbs/distributeFiles.py %s %s/%s %s" %(self.pythonExec, self.savePath, scratchOutputPath, image, baseName)

        print >>jobOut, "echo Moved %s files to %s/" %(self.obshistid, self.savePath)
        jobOut.close()
        return


    #def cleanNodeDir(pbsfilename, nodedir, policy, sensorid, filter):
    def writeCleanupCommands(self, jobFileName, wuID, sensorId):
        """
        Be a good boy/girl and clean up after yourself.

        """
        #tempHist, rNum, sNum, eNumber = wuID.split('_')
        #obshistid = re.sub('%s/' %(self.username),'', tempHist)
        try:
            jobOut = open(jobFileName, 'a')
        except IOError:
            print "Could not open %s for writing cleanup commands for PBS script" %(jobFileName)
            sys.exit()
        print >>jobOut, "### ---------------------------------------"
        print >>jobOut, "### DELETE the local node directories and all files."
        print >>jobOut, "### Does not delete parent directories if created"
        print >>jobOut, "### ---------------------------------------"
        print >>jobOut, "echo Now deleting files in %s/%s" %(self.scratchPath, wuID)
        print >>jobOut, "/bin/rm -rf %s/%s" %(self.scratchPath, wuID)
        print >>jobOut, "echo ---"
        if self.useDb == True:
            self.dbCleanup(jobOut, self.obshistid, sensorId)
        print >>jobOut, "echo single-chip job finished at `date`"
        print >>jobOut, " "
        print >>jobOut, "###"
        jobOut.close()
        return




class SingleChipScriptGenerator_Pbs(SingleChipScriptGenerator):
    """
    This is the PBS-specific class derived from the SingleChipScriptGenerator.
    The main differences are the addition of a few extra variables in __init__,
    the presence of Nicole's database commands, which require the LSST stack,
    and the job preamble, which contains a lot of PBS-specific info.

    Job execution, output transfer, and cleanup phases are the same as the
    master class.
    """

    def __init__(self, policy, obshistid, filter, filt, centid, centroidPath,
                 stagePath2, paramDir, trackingParFile):
        SingleChipScriptGenerator.__init__(self, policy, obshistid, filter, filt,
                                           centid, centroidPath, stagePath2,
                                           paramDir, trackingParFile)

        self.username = self.policy.get('pbs','username')
        # Path to the PBS-specific JOB DIRECTORIES running on the local nodes.
        # (These are redefined for PBS)
        #self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), self.username)
        print 'Your exec-node scratch Path is: ', self.scratchPath
        return


    def jobFileName(self, id):
        return 'exec_%s_%s.pbs' %(self.obshistid, id)

    """
    Database methods: dbSetup and dbCleanup

    These are from Nicole's original scripts and are kept
    within the PBS implementation because they use the LSST stack
    """

    def dbSetup(self, jobFile, sensorId):
        catGenPath = self.policy.get('lsst','catGen')

        jobFile.write('cd $PBS_O_WORKDIR \n')
        jobFile.write('echo Using Job Monitor Database. \n')
        jobFile.write('echo Setting up LSST throughputs and catalogs_generation packages. \n')
        jobFile.write('setup pex_policy \n')
        jobFile.write('setup pex_exceptions \n')
        jobFile.write('setup pex_logging \n')
        jobFile.write('setup throughputs 1.0 \n')
        jobFile.write('setup catalogs_generation \n')
        # minor setup hack necessary to allow cat_gen to work until
        # uprev to later numpy version than what is currently (as of 4/26/11) available
        jobFile.write('setup pyfits \n')
        jobFile.write('unsetup numpy \n')
        jobFile.write('setup python \n')
        jobFile.write('python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s running %s %s\n'%(self.catGenPath, self.obshistid, sensorId, self.username))
        #jobFile.write('python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s running %s %s\n'%(self.catGenPath, self.obshistid, sensorId, self.username, self.filter))
        jobFile.write('echo Updated the jobAllocator database with key: RUNNING. \n')
        return


    def dbCleanup(self, jobOut, obshistid, sensorid):
        catGenPath = self.policy.get('lsst','catGen')
        print >>jobOut, "cd $PBS_O_WORKDIR"
        try:
            #print >>jobOut, "  python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s finished %s %s" %(catGenPath, obshistid, sensorid, username, filter)
            print >>jobOut, "  python %s/python/lsst/sims/catalogs/generation/jobAllocator/myJobTracker.py %s finished %s %s" %(catGenPath, obshistid, sensorid, self.username)
            print >>jobOut, "  echo Updated the jobAllocator database with key: FINISHED"
        except OSError:
            print OSError
        return


    def writeSetupCommands(self, jobFileName, wuID, sensorId):
        """
        Redefinition of the writeSetupCommands method to do
        PBS-specific stuff.
        """

        self.header(jobFileName, wuID, sensorId)
        self.logging(jobFileName, wuID)
        #self.setupCleanup(pbsFileName, nodedir, self.policy, sensorId, self.filter)
        self.setupCleanup(jobFileName, wuID, sensorId)
        return


    def writeHeader(self, pbsfilename, wuID, sensorId, visitLogPath):
        """

        Write some typical PBS header file information.

        NOTES: The simulator jobs require only one node per job
        for PT1.2 (as of 5/3/11).  This may change in the future.

        """

        """
        saveDir          = policy.get('general','savedir')
        scratchpartition = policy.get('pbs','scratchpartition')
        """

        jobname          = self.policy.get('general','jobname')
        processors       = self.policy.getint('general','processors')
        nodes            = self.policy.getint('general','numNodes')
        pmem             = self.policy.getint('general','pmem')
        walltime         = self.policy.get('pbs','walltime')
        username         = self.policy.get('pbs','username')
        rootEmail        = self.policy.get('pbs','rootEmail')
        queueTmp         = self.policy.get('pbs','queue')


        if queueTmp == 'astro':
            queue = '-l qos=astro'
        else:
            queue = '-q %s' %(queueTmp)

        tempHist, rNum, sNum, eNumber = wuID.split('_')
        #obshistid = re.sub('%s/' %(username),'', tempHist)
        obshistid = self.obshistid
        eNum, filNum = eNumber.split('f')
        filename = obshistid + '_' + rNum + '_' + sNum + '_' + eNum

        sDate = str(datetime.datetime.now())
        paramdir = '%s-f%s' %(obshistid, self.filter)
        visitPath = os.path.join(self.savePath, paramdir)

        if os.path.isfile(pbsfilename):
            os.remove(pbsfilename)


        try:
            pbsout = open(pbsfilename, 'a')
        except IOError:
            print "Could not open %s for writing header info for the PBS script" %(pbsfilename)
            sys.exit()

        if self.debugLevel > 0:
            print >>pbsout, "#!/bin/csh -x"
        else:
            print >>pbsout, "#!/bin/csh"
        print >>pbsout, "### ---------------------------------------"
        print >>pbsout, "### PBS script created by: %s " %(username)
        print >>pbsout, "###              created on: %s " %(sDate)
        print >>pbsout, "### workUnitID: %s" %(wuID)
        print >>pbsout, "### obsHistID:  %s" %(self.obshistid)
        print >>pbsout, "### sensorID:   %s" %(sensorId)
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
        print >>pbsout, "#PBS -o %s/%s.out" %(visitLogPath, filename)
        print >>pbsout, "#PBS -l walltime=%s" %(walltime)
        print >>pbsout, "#PBS -l nodes=%s:ppn=%s" %(nodes, processors)
        print >>pbsout, "#PBS -l pmem=%sMB" %(pmem)
        print >>pbsout, "#PBS %s" %(queue)
        print >>pbsout, " "
        print >>pbsout, "### ---------------------------------------"
        print >>pbsout, "### Begin Imsim Executable Sections "
        print >>pbsout, "### ---------------------------------------"
        pbsout.write('echo Setting up the LSST Stack to get the proper version of Python. \n')
        pbsout.write('source /share/apps/lsst_gcc440/loadLSST.csh \n')
        pbsout.write('unalias cp \n')
        #pbsout.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
        pbsout.close()

        self.logging(pbsfilename, wuID)
        self.setupCleanup(pbsfilename, wuID, sensorId)

        return

    def logging(self, pbsfilename, wuID):
        """

        Write some useful logging and diagnostic information to the
        logfiles.

        """

        wuPath = os.path.join(self.scratchPath, wuID)

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
        if wuID != None:
            print >>pbsout, "echo The directory in which this job will run is %s" %(wuPath)
        else:
            #print >>pbsout, "echo No local node directory was indicated - job will run in `echo $PBS_O_WORKDIR`"
            print 'ERROR: You must specify a remote execution directory!'
            quit()
        print >>pbsout, "echo This job is running on `echo $num_procs` processors"
        print >>pbsout, "echo This job is starting at `date`"
        print >>pbsout, "echo ---"
        pbsout.close()
        return

    #def setupCleanup(pbsfilename, nodedir, policy, sensorid, filter):
    def setupCleanup(self, pbsfilename, wuID, sensorid):

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


        #scratchdir = '%s/%s' %(scratchpartition, nodedir)
        wuPath = os.path.join(self.scratchPath, wuID)
        imsimSourcePath = os.getenv("IMSIM_SOURCE_PATH")

        tempHist, rNum, sNum, eNumber = wuID.split('_')
        obshistid = re.sub('%s/' %(self.username),'', tempHist)

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
        print >>pbsout, "set local_scratch_dir = %s" %(wuPath)
        print >>pbsout, "set job_submission_dir = $PBS_O_WORKDIR"
        print >>pbsout, "set obshistid = %s" %(obshistid)
        if self.useDb == True:
            catGenPath = self.policy.get('lsst','catGen')
            print >>pbsout, "set cat_gen = %s" %(catGenPath)
        print >>pbsout, "set sensorid = %s" %(sensorid)
        print >>pbsout, "set username = %s" %(self.username)
        if self.useDb == True:
            print >>pbsout, "set minerva0_command = 'cd %s; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_error.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir',OBSHISTID='$obshistid',SENSORID='$sensorid',CAT_GEN='$cat_gen',USERNAME='$username" %(imsimSourcePath, wuID)
        else:
            print >>pbsout, "set minerva0_command = 'cd %s; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_files.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir" %(imsimSourcePath, wuID)
        print >>pbsout, "echo $minerva0_command"
        print >>pbsout, "#set pbs_output = `ssh minerva0 $minerva0_command`"
        print >>pbsout, "#set cleanup_job_id = `echo $pbs_output | awk -F. '{print $1}'`"
        print >>pbsout, "#echo I just submitted cleanup job ID $cleanup_job_id"
        print >>pbsout, "echo ---"
        print >>pbsout, " "
        pbsout.close()
        return
