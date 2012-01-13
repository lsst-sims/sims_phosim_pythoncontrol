#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

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
         5. cleanNodeDir: Copy/move all data to saveDir and delete node
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

class SingleVisitScriptGenerator:
    """
    Generates a script for doing the preprocessing steps for a single obsHistID.
    This class generates a generic shell script.  It can be used as a superclass
    for PBS- or Exacycle-specific script generation.
    """
    def __init__(self, scriptFileName, policy, obsHistID):
        self.scriptFileName = scriptFileName
        self.policy = policy
        self.obsHistID = obsHistID
        # Map filter number to filter character
        self.filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
        # Get ImSim version
        versionFile = 'raytrace/version'
        for line in open(versionFile).readlines():
            if line.startswith('Revision:'):
                name, self.revision = line.split()

        self.imsimHomePath = os.getenv("IMSIM_HOME_DIR")
        self.imsimDataPath = os.getenv("CAT_SHARE_DATA")

        #
        # Get [general] config file info
        #
        #policy   = pexPolicy.Policy.createPolicy(imsimPolicy)
        # Job params
        self.numNodes = self.policy.get('general','numNodes')
        self.processors = self.policy.get('general','processors')
        self.pmem = self.policy.get('general','pmem')
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        self.scratchPath = self.policy.get('general','scratchPath')
        self.scratchDataDir = self.policy.get('general','scratchDataDir')
        self.scratchDataPath = os.path.join(self.scratchPath, self.scratchDataDir)
        self.savePath  = self.policy.get('general','saveDir')
        self.stagePath  = self.policy.get('general','stagingDir')
        self.tarball  = self.policy.get('general','dataTarball')
        # Job monitor database
        self.useDatabase = self.policy.getboolean('general','useDatabase')

    def writeSetupCommands(self, stageDir, visitDir):

        username = getpass.getuser()
        try:
            with file(self.scriptFileName, 'a') as cshOut:
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Shell script created by: %s " %(username)
                print >>cshOut, "###              created on: %s " %(str(datetime.datetime.now()))
                print >>cshOut, "### Running SVN imsim revision %s." %(self.revision)
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "/bin/csh"
                print >>cshOut, " "
        except IOError:
            print "Could not open %s for writing shell script" %(self.scriptFileName)
            sys.exit()


    def writeJobCommands(self, trimfileName, trimfileBasename, trimfilePath,
                         filt, filter, obshistid, origObshistid, stagePath, extraIdFile,
                         imsimConfigFile, visitDir):

        """

        Add the actual job commands for doing the preprocessing steps.

        This is the script that will be run on the compute node.  Therefore, all
        pathnames should be from the perspective of that node.

        Your job should copy the necessary job files to scratchDataDir (in scratchPath),
        from stagingDir. Your job should ALSO copy the output from the preprocessing
        steps back to saveDir.

        """

        scratchPath = self.scratchPath
        scratchDataDir = self.scratchDataDir
        stagePath = self.stagePath

        try:
            with file(self.scriptFileName, 'a') as cshOut:

                print >>cshOut, " "
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Start your personal executable section"
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, " "
                
                # Copy data and node files
                cshOut.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
                cshOut.write('setenv IMSIM_HOME_DIR %s \n' %(self.imsimHomePath))
                #cshOut.write('echo Setting up the LSST Stack, pex_logging, _exceptions, and _policy packages. \n')
                #cshOut.write('source /share/apps/lsst_gcc440/loadLSST.csh \n')
                #cshOut.write('setup pex_policy \n')
                #cshOut.write('setup pex_exceptions \n')
                #cshOut.write('setup pex_logging \n')
                #cshOut.write('echo Sleeping for %s seconds. \n' %(myRandInt))
                #cshOut.write('sleep %s \n' %(myRandInt))
                #cshOut.write('cd $PBS_O_WORKDIR \n')
                # Make sure your working directory on the compute node exists
                cshOut.write('if (-d %s ) then \n' %(scratchPath))
                cshOut.write('  cd %s \n' %(scratchPath))
                cshOut.write('else \n')
                cshOut.write('  mkdir %s \n' %(scratchPath))
                cshOut.write('  cd %s \n' %(scratchPath))
                cshOut.write('endif \n')
                #cshOut.write('cd ../ \n')
                # Make sure the data directory and all files are present on the node.
                # The code assumes the data directory is "data" in the CWD from where it is run.
                cshOut.write('echo Initializing lock file. \n')
                cshOut.write('lockfile -l 1800 %s.lock \n' %(scratchDataDir))
                cshOut.write('if (-d %s/starSED/kurucz ) then \n' %(scratchDataDir))
                cshOut.write('  echo The new %s directory exists! \n' %(scratchDataDir))
                cshOut.write('else \n')
                cshOut.write('  echo The %s directory does not exist. Copying %s/%s. \n' %(scratchDataDir, self.imsimDataPath, self.tarball))
                cshOut.write('  rm -rf %s \n' %(scratchDataDir))
                cshOut.write('  cp %s/%s . \n' %(self.imsimDataPath, self.tarball))
                cshOut.write('  tar xzf %s \n' %(self.tarball))
                cshOut.write('  rm %s \n' %(self.tarball))
                cshOut.write('endif \n')
                # cshOut.write('cp $PBS_O_WORKDIR/verifyData.py . \n')
                # cshOut.write('python verifyData.py \n')
                cshOut.write('rm -f %s.lock \n' %(scratchDataDir))
                cshOut.write('echo Removed lock file and copying files for the node. \n')
                
                #
                # Copy trimfiles from staging
                #
                trimfileStagePath = os.path.join(stagePath,'trimfiles', visitDir)
                # Now copy the entire directory in trimfileOrigPath to the compute node
                cshOut.write('echo Copying %s to %s.\n' %(trimfileStagePath, scratchPath))
                cshOut.write('cp -a %s %s\n' %(trimfileStagePath, scratchPath))
                #
                # Copy visitFiles from staging
                #
                # "visitDir" is the destination directory on the compute node for this obshistid/filter
                # (this created during the copy of the trimfiles)
                visitPath = os.path.join(scratchPath, visitDir)
                cshOut.write('echo Copying and untarring visitFiles%s-f%s.tar.gz to %s\n'
                             %(obshistid, filt, visitPath))
                cshOut.write('cd %s \n' %(visitPath))
                cshOut.write('cp %s/visitFiles%s-f%s.tar.gz . \n' %(stagePath, obshistid, filt))
                cshOut.write('tar xzvf visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                cshOut.write('rm visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                cshOut.write('echo Setting soft link to %s directory. \n' %(scratchDataDir))
                cshOut.write('ln -s %s/%s/ %s \n' %(scratchPath, scratchDataDir, scratchDataDir))
                # JPG: I don't think we need to make this now, because it gets made in fullFocalPlane
                #cshOut.write('mkdir %s \n' %(nodeDataDir))
                cshOut.write('echo Running fullFocalplanePbs.py with %s. \n' %(extraIdFile))
                cshOut.write("python fullFocalplanePbs.py %s %s %s '' '' '' '' ''\n" %(trimfileBasename, imsimConfigFile, extraIdFile, ))
                cshOut.write('cp %s_f%sJobs.lis %s \n'%(obshistid, filt, self.savePath))


                #for lines in jobinput:
                #    print >>pbsout, "%s" %(lines)
                #jobinput.close()
                print >>cshOut, "echo ---"
                print >>cshOut, "echo job finished at `date`"

        except IOError:
            print "Could not open %s for writing jobCommands for shell script" %(self.scriptFileName)
            sys.exit()

        ## close file
        #pbsout.close()
        return

    def writeCleanupCommands(self, visitDir):

        visitPath = os.path.join(self.scratchPath, visitDir)
        try:
            with file(self.scriptFileName, 'a') as cshOut:
                print >>cshOut, "\n"
                print >>cshOut, "rm -rf %s\n" %(visitPath)
        except IOError:
            print "Could not open %s for writing header info for the shell script" %(self.scriptFileName)
            sys.exit()
 

    def tarVisitFiles(self, obshistid, filt, imsimConfigFile, extraIdFile):
        """

        Make tarball of needed files for local nodes, create
        directories on shared file system for logs and images. 

        Create tar files to be copied to local nodes.  This prevents
        IO issues with copying lots of little files from the head
        node to the local nodes.

        """
        os.chdir(self.imsimHomePath)
        visitFileTar = 'visitFiles%s-f%s.tar.gz' %(obshistid, filt)
        #visitFileGzip = '%s.gz' %(visitFileTar)
        cmd = 'tar czvf %s ancillary/atmosphere_parameters/* ancillary/atmosphere/cloud ancillary/atmosphere/turb2d ancillary/optics_parameters/optics_parameters ancillary/optics_parameters/control ancillary/trim/trim ancillary/Add_Background/add_background ancillary/Add_Background/filter_constants* ancillary/Add_Background/fits_files ancillary/Add_Background/SEDs/*.txt ancillary/Add_Background/update_filter_constants ancillary/Add_Background/vignetting_*.txt ancillary/cosmic_rays/create_rays ancillary/cosmic_rays/iray_textfiles/iray* ancillary/e2adc/e2adc ancillary/tracking/tracking raytrace/lsst raytrace/*.txt raytrace/version raytrace/setup pbs/distributeFiles.py chip.py fullFocalplanePbs.py makePbsFiles.py %s %s' %(visitFileTar, imsimConfigFile, extraIdFile)

        print 'Tarring all redundant files that will be copied to the execution node(s).'
        subprocess.check_call(cmd, shell=True)

        # Zip the tar file.
        #print 'Gzipping %s file' %(visitFileTar)
        #cmd = 'gzip %s' %(visitFileTar)
        #subprocess.check_call(cmd, shell=True)
        return visitFileTar

    def stageFiles(self, trimfileAbsName, trimfileBasename, trimfilePath,
                   filt, filter, obshistid, origObshistid, stagePath,
                   visitFileTgz, scriptFileName, scriptOutList, visitDir):
        # Move the visit and script files to stagedir.
        print 'Moving Visit & Script Files to %s:' %(stagePath)
        shutil.move(visitFileTgz, stagePath)
        shutil.move(scriptFileName, stagePath)

        # Also stage the trimfiles to stagePath/trimfiles/
        trimfileStageDir = os.path.join(stagePath,'trimfiles', visitDir)
        print 'Staging trimfile %s to %s:' %(trimfileAbsName, trimfileStageDir)
        os.mkdir(trimfileStageDir)
        shutil.copy(trimfileAbsName, trimfileStageDir)
        # Make sure that there is at least one "includeobj" line in trimfile
        cmd = ('grep includeobj %s' %(trimfileAbsName))
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        nincobj = len(results)
        # If so, stage the files in the pops directory for this object 
        if nincobj > 0:
            popsPath = os.path.join(trimfilePath, 'pops') # Orig location for 'pops' dir
            dest = '%s/pops' %(trimfileStageDir)
            os.mkdir(dest)  # Create dest dir
            # Copy the file glob with origObshistid
            for singleFile in glob.glob('%s/*%s*' %(popsPath, origObshistid)):
                print '   Moving %s to %s/pops' %(singleFile, dest)
                shutil.copy(singleFile, dest)
            
        # Generate the list of job scripts for the ray tracing and post processing
        fileDest = os.path.join(stagePath, scriptFileName)
        print 'Attempting to add %s to file %s in %s' %(fileDest, scriptOutList, self.imsimHomePath)
        os.chdir(self.imsimHomePath)
        with file(scriptOutList, 'a') as parFile:
            parFile.write('%s \n' %(fileDest))
        


class SingleVisitPbsGenerator(SingleVisitScriptGenerator):

    def __init__(self, scriptFileName, policy, obsHistID):
        """
        Augment the superclass's constructor because Nicole has the PBS implementation
        expecting 'scratchPath' to have the PBS 'username' appended to it.
        """
        SingleVisitScriptGenerator.__init__(self, scriptFileName, policy, obsHistID)
        username = self.policy.get('pbs','username')
        self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), username)


    def header(self, filter):

        """
        
        Write some typical PBS header file information after obtaining
        necessary parameters from the given policy file.
        
        """
        policy = self.policy
        obshistid = self.obsHistID

        saveDir = self.savePath
        processors = self.processors
        nodes = self.numNodes
        pmem = self.pmem
        jobname = self.jobName
        walltime         = policy.get('pbs','walltime')
        username         = policy.get('pbs','username')
        scratchpartition = policy.get('general','scratchPath')
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
            with file(self.scriptFileName, 'a') as pbsOut:
                print >>pbsOut, "#!/bin/csh -x"
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
                print >>pbsOut, "#PBS -o %s/logs/%s.out" %(savePath, filename)
                print >>pbsOut, "#PBS -l walltime=%s" %(walltime)
                print >>pbsOut, "#PBS -l nodes=%s:ppn=%s" %(nodes, processors)
                print >>pbsOut, "#PBS -l pmem=%sMB" %(pmem)
                print >>pbsOut, "#PBS %s" %(queue)
                print >>pbsOut, " "
                print >>pbsOut, "### ---------------------------------------"
                print >>pbsOut, "### Begin Imsim Executable Sections "
                print >>pbsOut, "### ---------------------------------------"
                print >>pbsOut, "unalias cp "
        except IOError:
            print "Could not open %s for writing header info for the PBS script" %(self.scriptFileName)
            sys.exit()


    def logging(self, visitDir):

        """

        Have the script write some useful logging and diagnostic information to the
        logfiles.

        """
        pbsfilename = self.scriptFileName
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

    def setupCleanup(self, visitDir):

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
        pbsfilename = self.scriptFileName
        policy = self.policy

        visitPath = os.path.join(self.scratchPath, visitDir)
        scratchpartition = self.scratchPath

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
        print >>pbsout, "set local_scratch_dir = %s" %(visitPath)
        print >>pbsout, "set job_submission_dir = $PBS_O_WORKDIR"
        print >>pbsout, "set minerva0_command = 'cd '$job_submission_dir'; /opt/torque/bin/qsub -N clean.%s -W depend=afternotok:'$pbs_job_id'  pbs/cleanup_files.csh -v CLEAN_MASTER_NODE_ID='$master_node_id',CLEAN_LOCAL_SCRATCH_DIR='$local_scratch_dir" %(visitDir)
        print >>pbsout, "echo $minerva0_command"
        print >>pbsout, "set pbs_output = `ssh minerva0 $minerva0_command`"
        print >>pbsout, "set cleanup_job_id = `echo $pbs_output | awk -F. '{print $1}'`"
        print >>pbsout, "echo I just submitted cleanup job ID $cleanup_job_id"
        print >>pbsout, "echo ---"
        print >>pbsout, " "
        # create local directories
        print >>pbsout, "## create local node directories (visitDir = %s)" %(visitPath)
        # check if directory already exists - remember you're writing to a csh script
        print >>pbsout, "if (! -d %s) then" %(self.scratchPath)
        print >>pbsout, "  echo 'Are you sure you're on a node?'; exit 1"
        print >>pbsout, "endif"
        print >>pbsout, "if (! -d %s) then" %(visitPath) # see if directory exists
        print >>pbsout, "  mkdir -p %s" %(visitPath)  # make the directory (including parents)
        # note that this will overwrite previous files
        print >>pbsout, "endif"
        print >>pbsout, "if (! -d %s) then" %(visitPath) # check if directory creation worked
        print >>pbsout, "  echo 'Something failed in creating local directory %s. Exiting.'" %(visitPath)
        print >>pbsout, "  exit 1"
        print >>pbsout, "endif"
        print >>pbsout, "cd %s" %(visitPath)
        # close file
        pbsout.close()
        return

    def writeJobCommands(self, trimfileName, trimfileBasename, trimfilePath,
                         filt, filter, obshistid, origObshistid, stagePath, extraIdFile,
                         imsimConfigFile, visitDir):

        """

        Add the actual job commands.

        NOTE: At this point, you are in the /state/partition/nodedir.
        Your job should copy the necessary job files to here (likely
        already done in the commandfile).  Your job should ALSO copy the
        output back to the share (not here on the node) location.

        """
        #pbsfilename = self.scriptFileName

        #try:
        #    pbsout = open(pbsfilename, 'a')
        #except IOError:
        #    print "Could not open %s for writing jobCommands for PBS script" %(pbsfilename)
        #    sys.exit()

        #print >>pbsout, " "
        #print >>pbsout, "### ---------------------------------------"
        #print >>pbsout, "### Start your personal executable section"
        #print >>pbsout, "### ---------------------------------------"
        #print >>pbsout, " "

        #try:
        #    jobinput = open(commandfile, 'r')
        #except IOError:
        #    print "Could not open %s for reading tcsh script for PBS job" %(commandfile)
        #    sys.exit()
        #if nodedir == None:
        #    print >>pbsout, "cd $PBS_O_WORKDIR"
        #myCmdFile = 'visitCmds_%s.txt' %(obshistid)

        visitPath = os.path.join(self.scratchPath, visitDir)
        username = self.policy.get('pbs','username')

        myRandInt = random.randint(0,60)

        try:
            with file(self.scriptFileName, 'a') as pbsOut:

                print >>pbsOut, " "
                print >>pbsOut, "### ---------------------------------------"
                print >>pbsOut, "### Start your personal executable section"
                print >>pbsOut, "### ---------------------------------------"
                print >>pbsOut, " "

                # Copy data and node files
                #JPG pbsOut.write('tcsh \n')
                pbsOut.write('setenv CAT_SHARE_DATA %s \n' %(self.imsimDataPath))
                pbsOut.write('setenv IMSIM_HOME_DIR %s \n' %(self.imsimHomePath))
                pbsOut.write('echo Setting up the LSST Stack, pex_logging, _exceptions, and _policy packages. \n')
                pbsOut.write('source /share/apps/lsst_gcc440/loadLSST.csh \n')
                #pbsOut.write('setup pex_policy \n')
                #pbsOut.write('setup pex_exceptions \n')
                #pbsOut.write('setup pex_logging \n')
                pbsOut.write('echo Sleeping for %s seconds. \n' %(myRandInt))
                pbsOut.write('sleep %s \n' %(myRandInt))
                pbsOut.write('cd $PBS_O_WORKDIR \n')
                # Make sure your working directory on the compute node exists
                pbsOut.write('if (-d %s ) then \n' %(self.scratchPath))
                pbsOut.write('  cd %s \n' %(self.scratchPath))
                pbsOut.write('else \n')
                pbsOut.write('  mkdir %s \n' %(self.scratchPath))
                pbsOut.write('  cd %s \n' %(self.scratchPath))
                pbsOut.write('endif \n')
                # Make sure the data directory and all files are present on the node.
                # Use relative path names so we can get to the shared scratch space on all nodes.
                # Code assumes the data directory scratchPath is scratchPath/../scratchDataDir
                pbsOut.write('cd ../ \n')
                pbsOut.write('echo Initializing lock file. \n')
                pbsOut.write('lockfile -l 1800 %s.lock \n' %(self.scratchDataDir))
                pbsOut.write('if (-d %s/starSED/kurucz ) then \n' %(self.scratchDataPath))
                pbsOut.write('  echo The data directory %s exists! \n' %(self.scratchDataPath))
                pbsOut.write('else \n')
                pbsOut.write('  echo The data directory %s does not exist. Copying %s. \n' %(self.scratchDataPath, os.path.join(self.imsimDataPath, self.tarball)))
                pbsOut.write('  cp %s . \n' %(os.path.join(self.imsimDataPath, self.tarball)))
                pbsOut.write('  tar xzf %s \n' %(self.tarball))
                pbsOut.write('  rm %s \n' %(self.tarball))
                pbsOut.write('endif \n')
                # pbsOut.write('cp $PBS_O_WORKDIR/verifyData.py . \n')
                # pbsOut.write('python verifyData.py \n')
                pbsOut.write('rm -f %s.lock \n' %(self.scratchDataDir))
                pbsOut.write('echo Removed lock file and copying files for the node. \n')
                pbsOut.write('cd $PBS_O_WORKDIR \n')
                # Copy Files needed for fullFocalplane.py
                pbsOut.write('echo Copying %s to %s.\n' %(trimfileName, visitPath))
                pbsOut.write('cp %s %s \n' %(trimfileName, visitPath))
                # Verify that this will always be the directory name/location
                cmd = ('grep includeobj %s' %(trimfileName))
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
                results = p.stdout.readlines()
                p.stdout.close()
                nincobj = len(results)
                if nincobj > 0:
                    popsPath = os.path.join(trimfilePath, 'pops')
                    pbsOut.write('echo Copying %s/*%s* files to %s.\n' %(popsPath, origObshistid, visitPath))
                    popsWritePath = os.path.join(visitPath, 'pops')
                    pbsOut.write('mkdir %s \n' %(popsWritePath))
                    pbsOut.write('cp %s/*%s* %s \n' %(popsPath, origObshistid, popsWritePath))
                pbsOut.write('cp %s/visitFiles%s-f%s.tar.gz %s \n' %(stagePath, obshistid, filt, visitPath))
                pbsOut.write('cd %s \n' %(visitPath))
                #pbsOut.write('gunzip visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                pbsOut.write('tar xzvf visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                pbsOut.write('rm visitFiles%s-f%s.tar.gz \n' %(obshistid, filt))
                pbsOut.write('echo Setting soft link to %s directory. \n' %(self.scratchDataDir))
                pbsOut.write('ln -s %s/ %s \n' %(self.scratchDataPath, self.scratchDataDir))
                # scratchOutputPath gets made in fullFocalPlane
                #pbsOut.write('mkdir %s \n' %(self.scratchOutputPath))
                pbsOut.write('echo Running fullFocalplanePbs.py with %s. \n' %(extraIdFile))
                pbsOut.write("python fullFocalplanePbs.py %s %s %s '' '' '' '' ''\n" %(trimfileBasename, imsimConfigFile, extraIdFile, ))
                pbsOut.write('cp %s_f%sPbsJobs.lis $PBS_O_WORKDIR \n'%(obshistid, filt))

                #for lines in jobinput:
                #    print >>pbsout, "%s" %(lines)
                #jobinput.close()

        except IOError:
            print "Could not open %s for writing jobCommands for PBS script" %(self.scriptFileName)
            sys.exit()

        ## close file
        #pbsout.close()
        return

    def cleanNodeDir(self, visitDir):
        """

        Be a good Cluster citizen...leave no trace.  Add the tcsh job
        commands to remove the directories from the node (for normal
        script operation - nonfailure mode).

        """

        visitPath = os.path.join(self.scratchPath, visitDir)

        try:
            with file(self.scriptFileName, 'a') as pbsOut:
                print >>pbsOut, "\n### ---------------------------------------"
                print >>pbsOut, "### DELETE the local node directories and all files"
                print >>pbsOut, "### (does not delete parent directories if created"
                print >>pbsOut, "### ---------------------------------------\n"
                print >>pbsOut, "echo Now deleting files in %s" %(visitPath)
                print >>pbsOut, "/bin/rm -rf %s" %(visitPath)
                print >>pbsOut, "echo ---"
                print >>pbsOut, "cd $PBS_O_WORKDIR"
                print >>pbsOut, "echo PBS job finished at `date`"
                print >>pbsOut, " "
                print >>pbsOut, "###"
        except IOError:
            print "Could not open %s for writing cleanup commands for PBS script" %(self.scriptFileName)
            sys.exit()

        return


        


class AllVisitsScriptGenerator:
    """
    Main class for generating the shell scripts for all of the visits/trimfiles
    that have been submitted.  Calls methods from SingleVisitScriptGenerator.
    This can be used as a superclass for PBS- or Exacycle-specific script
    generators.
    """

    def __init__(self, myfile, imsimConfigFile, extraIdFile):
        self.imsimHomePath = os.getenv("IMSIM_HOME_DIR")
        if self.imsimHomePath is None:
            raise NameError('Could not find value for IMSIM_HOME_DIR.')
        self.imsimDataPath = os.getenv("CAT_SHARE_DATA")
        if self.imsimDataPath is None:
            raise NameError('Could not find value for CAT_SHARE_DATA.')
        self.imsimConfigFile = imsimConfigFile

        # map filter number to filter character
        self.filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}

        #
        # Get necessary config file info
        #
        self.policy = ConfigParser.RawConfigParser()
        self.policy.read(imsimConfigFile)
        #policy   = pexPolicy.Policy.createPolicy(imsimPolicy)
        # Job params
        self.numNodes = self.policy.get('general','numNodes')
        self.processors = self.policy.get('general','processors')
        self.pmem = self.policy.get('general','pmem')
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        self.scratchPath = self.policy.get('general','scratchPath')
        self.scratchDataDir = self.policy.get('general','scratchDataDir')
        self.savePath  = self.policy.get('general','saveDir')
        self.stagePath  = self.policy.get('general','stagingDir')
        self.tarball  = self.policy.get('general','dataTarball')
        # Job monitor database
        self.useDatabase = self.policy.getboolean('general','useDatabase')

        print "scratchDataDir:   %s" %(self.scratchDataDir)
        #
        # Load list of trimfiles
        #
        myFiles = '%s' %(myfile)
        # files = open(myFiles).readlines()
        self.trimfileList = open(myFiles).readlines()


        #JPG: I am not certain if extraIdFile can have more than one line with
        #     "extraid".  The loop below replicates the original behavior of
        #     Nicole's code in case there can be more than one line.
        self.extraIdFile = extraIdFile.strip()
        self.extraid = ''
        if self.extraIdFile != '':
            for line in open(self.extraIdFile).readlines():
                if line.startswith('extraid'):
                    name, extraid_tmp = line.split()
                    print 'extraid:', extraid_tmp
                    if extraid_tmp != '':
                        #JPG: Note that this is a string addition operator
                        self.extraid = self.extraid + extraid_tmp
        print 'Final extraid:', self.extraid
                        


    def makeScripts(self):
        """
        Loops over trimfiles in trimfileList and calls scriptWriter to output the
        actual script.
        """
        self.checkDirectories()
        for trimfileName in self.trimfileList:
            trimfileName = trimfileName.strip()
            trimfileBasename = os.path.basename(trimfileName)
            trimfilePath = os.path.dirname(trimfileName)
            #basename, extension = os.path.splitext(trimfileName)

            for line in open(trimfileName).readlines():
                if line.startswith('Opsim_filter'):
                    name, filter = line.split()
                    print 'Opsim_filter:', filter
                if line.startswith('Opsim_obshistid'):
                    name, obshistid = line.split()
                    print 'Opsim_obshistid:', obshistid

            ono = list(obshistid)
            if len(ono) > 8:
                origObshistid = '%s%s%s%s%s%s%s%s' %(ono[0], ono[1], ono[2], ono[3], ono[4],ono[5], ono[6], ono[7])
            else:
                origObshistid = obshistid

            # Add in extraid
            obshistid = obshistid + self.extraid

            # Create SAVE & LOG DIRECTORIES for this visit
            filt = self.filtmap[filter]
            visitDir = '%s-f%s' %(obshistid, filt)
            visitSavePath = os.path.join(self.savePath, visitDir)
            visitLogPath = os.path.join(visitSavePath, 'logs')
            runName = 'run%s' %(obshistid)
            visitParamPath = os.path.join(visitSavePath, runName)

            self.checkVisitDirectories(visitSavePath, visitLogPath, visitParamPath)
            self.scriptWriter(trimfileName, trimfileBasename, trimfilePath, filt, filter, obshistid, origObshistid)


    def checkDirectories(self):
        # Checks directories accessible from the client that are used for all visits
        stagePath = self.stagePath
        # Remove stagePath if it exists
        if os.path.isdir(stagePath):
            print 'Removing %s' %(stagePath)
            shutil.rmtree(stagePath)
        try:
            print 'Creating %s' %(stagePath)
            os.makedirs(stagePath)
        except OSError:
            print OSError
        # Now create trimfiles staging directory, too
        trimfileStagePath = os.path.join(stagePath, 'trimfiles')
        print 'Creating %s' %(trimfileStagePath)
        try:
            os.makedirs(trimfileStagePath)
        except OSError:
            print OSError
        
        
    def checkVisitDirectories(self, savePath, logPath, paramDir):
        # Checks the visit-specific directories accessible from the client
        print 'Creating visit directories if necessary:'
        print '--- Checking visit save directory %s.' %(savePath)
        if not os.path.isdir(savePath):
            try:
                os.makedirs(savePath)
                print '------Making visit save directory: %s' %(savePath)
            except OSError:
                print OSError

        print '--- Checking visit log directory %s.' %(logPath)
        if not os.path.isdir(logPath):
            try:
                os.makedirs(logPath)
                print '------Making visit log directory: %s' %(logPath)
            except OSError:
                print OSError

        print '--- Checking visit "run" directory %s.' %(paramDir)
        if not os.path.isdir(paramDir):
            try:
                os.makedirs(paramDir)
                print '------Making visit "run" directory: %s' %(paramDir)
            except OSError:
                print OSError

        




    def scriptWriter(self, trimfileName, trimfileBasename, trimfilePath,
                     filt, filter, obshistid, origObshistid):
        """
        This is called by makeScripts() and actually generates the script
        text for each trimfile.
        As part of the AllVisitsScriptGenerator class, this writes a generic
        shell script.  Subclasses for PBS or Exacycle can redefine this
        method for their own specific implementations.
        """

        # visitDir is the directory within scratchPath that contains info for the particular objhistid + filter
        visitDir = '%s-f%s' %(obshistid, filter)


        # Make the csh script for this visit
        scriptFileName = '%s_f%s.csh' %(obshistid, filt)
        scriptGen = SingleVisitScriptGenerator(scriptFileName, self.policy, obshistid)
        scriptGen.writeSetupCommands(self.stagePath, visitDir)
        scriptGen.writeJobCommands(trimfileName, trimfileBasename, trimfilePath,
                                  filt, filter, obshistid, origObshistid, self.stagePath, self.extraIdFile,
                                  self.imsimConfigFile, visitDir)
        scriptGen.writeCleanupCommands(visitDir)

        visitFileTgz = scriptGen.tarVisitFiles(obshistid, filt, self.imsimConfigFile, self.extraIdFile)
        scriptOutList = 'genFilesToRun_%s.lis' %(self.extraIdFile)
        scriptGen.stageFiles(trimfileName, trimfileBasename, trimfilePath,
                             filt, filter, obshistid, origObshistid, self.stagePath,
                             visitFileTgz, scriptFileName, scriptOutList, visitDir)


class AllVisitsPbsGenerator(AllVisitsScriptGenerator):
    """
    This class redefines scriptWriter() for PBS.
    """

    def __init__(self, myfile, imsimConfigFile, extraIdFile):
        """
        Augment the superclass's constructor because Nicole has the PBS implementation
        expecting 'scratchPath' to have the PBS 'username' appended to it.
        """
        AllVisitsScriptGenerator.__init__(self, myfile, imsimConfigFile, extraIdFile)
        username = self.policy.get('pbs','username')
        self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), username)


    def scriptWriter(self, trimfileName, trimfileBasename, trimfilePath,
                     filt, filter, obshistid, origObshistid):

        username = self.policy.get('pbs','username')

        # Make the PBS file for this visit
        pbsFileName = '%s_f%s.pbs' %(obshistid, filt)
        # visitDir is the directory that contains info for the particular objhistid + filter
        visitDir = '%s-f%s' %(obshistid, filter)
        # scratchDir is '$username/$visitDir' (this is confusing, so maybe get rid of the automagic
        #      creation of the $username subdirectories for PBS).  This is often called
        #      'nodedir' in some of Nicole's remaining code.
        scratchDir = os.path.join(username, visitDir)

        pbsGen = SingleVisitPbsGenerator(pbsFileName, self.policy, obshistid)
        pbsGen.header(filter)
        pbsGen.logging(visitDir)
        pbsGen.setupCleanup(visitDir)
        pbsGen.writeJobCommands(trimfileName, trimfileBasename, trimfilePath,
                               filt, filter, obshistid, origObshistid, self.stagePath, self.extraIdFile,
                               self.imsimConfigFile, visitDir)
        pbsGen.cleanNodeDir(visitDir)
        print "Created PBS file %s" %(pbsFileName)


        visitFileTgz = pbsGen.tarVisitFiles(obshistid, filt, self.imsimConfigFile, self.extraIdFile)
        # Remove the command file.
        #print 'Removing %s.' %(myCmdFile)
        #os.remove(myCmdFile)
        pbsOutList = 'genFilesToSubmit_%s.lis' %(self.extraIdFile)
        pbsGen.stageFiles(trimfileName, trimfileBasename, trimfilePath,
                          filt, filter, obshistid, origObshistid, self.stagePath,
                          visitFileTgz, pbsFileName, pbsOutList, visitDir)

        return

