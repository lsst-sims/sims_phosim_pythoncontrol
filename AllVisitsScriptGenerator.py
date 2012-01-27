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
from SingleVisitScriptGenerator import *

class AllVisitsScriptGenerator:
    """
    Main class for generating the shell scripts for all of the visits/trimfiles
    that have been submitted.  Calls methods from SingleVisitScriptGenerator.
    This can be used as a superclass for PBS- or Exacycle-specific script
    generators.
    """

    def __init__(self, myfile, policy, imsimConfigFile, extraIdFile):

        self.policy = policy

        # We want to track the path where the script was originally
        # invoked because we will have to cd to other directories
        # temporarily.
        self.scriptInvocationPath = os.getcwd()
        self.imsimHomePath = os.getenv("IMSIM_HOME_DIR")
        if self.imsimHomePath is None:
            raise NameError('Could not find value for IMSIM_HOME_DIR.')
        self.imsimDataPath = os.getenv("CAT_SHARE_DATA")
        if self.imsimDataPath is None:
            raise NameError('Could not find value for CAT_SHARE_DATA.')
        self.imsimConfigFile = imsimConfigFile

        # map filter number to filter character
        self.filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}

        #policy   = pexPolicy.Policy.createPolicy(imsimPolicy)
        # Job params
        self.numNodes = self.policy.get('general','numNodes')
        self.processors = self.policy.get('general','processors')
        self.pmem = self.policy.get('general','pmem')
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        self.scratchPath = self.policy.get('general','scratchPath')
        self.scratchDataDir = self.policy.get('general','scratchDataDir')
        self.savePath  = self.policy.get('general','savePath')
        self.stagePath  = self.policy.get('general','stagingPath1')
        self.stagePath2  = self.policy.get('general','stagingPath2')
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
        Loops over trimfiles in trimfileList and calls processTrimFile which reads
        in the trimfile and then calls scriptGen.makeScript() to generate the actual
        script.
        """
        self.checkDirectories()        

        # Remove the file containing the script names if it exists.
        scriptOutList = 'visitScriptsToRun_%s.lis' %(self.extraIdFile)
        scriptFile = os.path.join(self.scriptInvocationPath, scriptOutList)
        if os.path.isfile(scriptFile):
            try:
                os.remove(scriptFile)
            except OSError:
                pass
        
        # Note that tarExecFiles() needs to be called before initializing
        # SingleVisitScriptGenerator because it defines self.execFileTgzName
        self.tarExecFiles()
        # SingleVisitScriptGenerator can be instantiated only once per execution environment,
        # So initialize it here, then call the makeScript() in the loop over trim files.
        scriptGen = SingleVisitScriptGenerator(self.scriptInvocationPath, scriptOutList, self.policy,
                                               self.imsiConfigFile, self.extraIdFile,
                                               self.execFileTgzName)
        
        for trimfileName in self.trimfileList:
            trimfileName = trimfileName.strip()
            self.processTrimFile(scriptGen, trimFileName)
        return


    def processTrimFile(self, scriptGen, trimfileName):
        # trimfileName is the fully-qualified name of the trimfile we are processing
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
        visitSavePath = os.path.join(self.stagePath2, visitDir)
        visitLogPath = os.path.join(self.savePath, visitDir, 'logs')
        visitParamDir = 'run%s' %(obshistid)  # Subdirectory within visitSavePath to store param files
        trimfileStagePath = os.path.join(self.stagePath, 'trimfiles', visitDir)

        self.checkVisitDirectories(visitSavePath, visitLogPath, visitParamDir, trimfileStagePath)
        scriptGen.makeScript(obshistid, origObshistid, trimfileName, trimfileBasename,
                             trimfilePath, filt, filter, visitDir, visitLogPath)
        #self.scriptWriter(trimfileName, trimfileBasename, trimfilePath, filt, filter, obshistid, origObshistid)
        return

    def checkDirectories(self):
        # Checks directories accessible from the client that are used for all visits
        stagePath = self.stagePath
        # Creat stagePath if it does not exist
        if not os.path.isdir(stagePath):
            try:
                print 'Creating %s' %(stagePath)
                os.makedirs(stagePath)
            except OSError:
                print OSError
        # Now create trimfiles staging directory, too
        trimfileStagePath = os.path.join(stagePath, 'trimfiles')
        if not os.path.isdir(trimfileStagePath):
            print 'Creating %s' %(trimfileStagePath)
            try:
                os.makedirs(trimfileStagePath)
            except OSError:
                print OSError
        return
       
        
    def checkVisitDirectories(self, visitSavePath, visitLogPath, visitParamDir, visitTrimfilePath):
        # Checks the visit-specific directories accessible from the client
        print 'Creating visit directories (and removing old ones if necessary):'
        print '--- Checking visit save directory %s.' %(visitSavePath)
        paramPath = os.path.join(visitSavePath, visitParamDir)
        if os.path.isdir(visitSavePath):
            print '------Removing visit save directory: %s' %(visitSavePath)
            shutil.rmtree(visitSavePath)
        try:
            print '------Making visit save directory: %s' %(visitSavePath)
            os.makedirs(visitSavePath)
            print '------Making param subdirectory: %s' %(paramPath)
            os.makedirs(paramPath)
        except OSError:
            print OSError

        print '--- Checking visit log directory %s.' %(visitLogPath)
        if os.path.isdir(visitLogPath):
            print '------Removing visit log directory: %s' %(visitLogPath)
            shutil.rmtree(visitLogPath)
        try:
            os.makedirs(visitLogPath)
            print '------Making visit log directory: %s' %(visitLogPath)
        except OSError:
            print OSError

        print '--- Checking visit trimfile stage directory %s.' %(visitTrimfilePath)
        if os.path.isdir(visitTrimfilePath):
            print '------Removing visit trimfile stage directory: %s' %(visitTrimfilePath)
            shutil.rmtree(visitTrimfilePath)
        try:
            os.makedirs(visitTrimfilePath)
            print '------Making visit trimfile stage directory: %s' %(visitTrimfilePath)
        except OSError:
            print OSError

        return
        

    def tarExecFiles(self):
        # None of the files in the source tree should be visit-dependent
        # Therefore, tar it up once at the beginning for all visits
        # NOTE TO SELF: Possibly use random file name while in source dir to
        #               avoid collision with other script invocations.
        os.chdir(self.imsimHomePath)
        self.execFileTgzName = 'imsimExecFiles.tar.gz'
        cmd = 'tar czvf %s ancillary/atmosphere_parameters/* ancillary/atmosphere/cloud ancillary/atmosphere/turb2d ancillary/optics_parameters/optics_parameters ancillary/optics_parameters/control ancillary/trim/trim ancillary/Add_Background/add_background ancillary/Add_Background/filter_constants* ancillary/Add_Background/fits_files ancillary/Add_Background/SEDs/*.txt ancillary/Add_Background/update_filter_constants ancillary/Add_Background/vignetting_*.txt ancillary/cosmic_rays/create_rays ancillary/cosmic_rays/iray_textfiles/iray* ancillary/e2adc/e2adc ancillary/tracking/tracking raytrace/lsst raytrace/*.txt raytrace/version raytrace/setup pbs/distributeFiles.py' %(self.execFileTgzName)
        
        print 'Tarring all exec files.'
        subprocess.check_call(cmd, shell=True)

        # Zip the tar file.
        #print 'Gzipping %s file' %(visitFileTar)
        #cmd = 'gzip %s' %(visitFileTar)
        #subprocess.check_call(cmd, shell=True)

        # Move the tarball to the invocation directory to minimize the time spent
        # in the source dir.
        shutil.copy(self.execFileTgzName, self.scriptInvocationPath)
        # cd back to the invocation directory
        os.chdir(self.scriptInvocationPath)



    def scriptWriterOLD(self, trimfileName, trimfileBasename, trimfilePath,
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
        scriptGen = SingleVisitScriptGenerator(self.scriptInvocationPath, scriptFileName,
                                               self.policy, obshistid)
        scriptGen.writeSetupCommands(self.stagePath, visitDir)
        scriptGen.writeJobCommands(trimfileName, trimfileBasename, trimfilePath,
                                  filt, filter, obshistid, origObshistid, self.stagePath, self.extraIdFile,
                                  self.imsimConfigFile, visitDir, self.execFileTgzName)
        scriptGen.writeCleanupCommands(visitDir)

        visitFileTgz = scriptGen.tarVisitFiles(obshistid, filt, self.imsimConfigFile, self.extraIdFile)
        scriptOutList = 'genFilesToRun_%s.lis' %(self.extraIdFile)
        scriptGen.stageFiles(trimfileName, trimfileBasename, trimfilePath,
                             filt, filter, obshistid, origObshistid, self.stagePath,
                             self.execFileTgzName,
                             visitFileTgz, scriptFileName, scriptOutList, visitDir)


class AllVisitsScriptGenerator_Pbs(AllVisitsScriptGenerator):
    """
    This class redefines scriptWriter() for PBS.
    """

    def __init__(self, myfile, policy, imsimConfigFile, extraIdFile):
        """
        Augment the superclass's constructor because Nicole has the PBS implementation
        expecting 'scratchPath' to have the PBS 'username' appended to it.
        """
        AllVisitsScriptGenerator.__init__(self, myfile, policy, imsimConfigFile, extraIdFile)
        # Check to make sure we are the correct class for the "scheduler1" option
        assert self.policy.get('general','scheduler1') == 'pbs'
        username = self.policy.get('pbs','username')
        # Redefine scratchPath to include username.
        self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), username)


    def makeScripts(self):
        """
        Loops over trimfiles in trimfileList and calls processTrimFile which reads
        in the trimfile and then calls scriptGen.makeScript() to generate the actual
        script.
        """
        self.checkDirectories()

        # Remove the file containing the script names if it exists.
        scriptOutList = 'visitScriptsToRun_%s.lis' %(self.extraIdFile)
        scriptFile = os.path.join(self.scriptInvocationPath, scriptOutList)
        if os.path.isfile(scriptFile):
            try:
                os.remove(scriptFile)
            except OSError:
                pass
        
        # Note that tarExecFiles() needs to be called before initializing
        # SingleVisitScriptGenerator because it defines self.execFileTgzName
        self.tarExecFiles()
        # SingleVisitScriptGenerator can be instantiated only once per execution environment,
        # So initialize it here, then call the makeScript() in the loop over trim files.
        scriptGen = SingleVisitScriptGenerator_Pbs(self.scriptInvocationPath, scriptOutList, self.policy,
                                               self.imsimConfigFile, self.extraIdFile,
                                               self.execFileTgzName)
        
        for trimfileName in self.trimfileList:
            trimfileName = trimfileName.strip()
            self.processTrimFile(scriptGen, trimfileName)
        return


    def scriptWriterOLD(self, trimfileName, trimfileBasename, trimfilePath,
                     filt, filter, obshistid, origObshistid):

        # Make the PBS file for this visit
        pbsFileName = '%s_f%s.pbs' %(obshistid, filt)
        # visitDir is the directory that contains info for the particular objhistid + filter
        visitDir = '%s-f%s' %(obshistid, filter)

        pbsGen = SingleVisitPbsGenerator(self.scriptInvocationPath, pbsFileName, self.policy, obshistid)
        pbsGen.header(filter)
        pbsGen.logging(visitDir)
        pbsGen.setupCleanup(visitDir)
        pbsGen.writeJobCommands(trimfileName, trimfileBasename, trimfilePath,
                               filt, filter, obshistid, origObshistid, self.stagePath, self.extraIdFile,
                               self.imsimConfigFile, visitDir, self.execFileTgzName)
        pbsGen.cleanNodeDir(visitDir)
        print "Created PBS file %s" %(pbsFileName)


        visitFileTgz = pbsGen.tarVisitFiles(obshistid, filt, self.imsimConfigFile, self.extraIdFile)
        # Remove the command file.
        #print 'Removing %s.' %(myCmdFile)
        #os.remove(myCmdFile)
        pbsOutList = 'genFilesToSubmit_%s.lis' %(self.extraIdFile)
        pbsGen.stageFiles(trimfileName, trimfileBasename, trimfilePath,
                          filt, filter, obshistid, origObshistid, self.stagePath,
                          self.execFileTgzName,
                          visitFileTgz, pbsFileName, pbsOutList, visitDir)

        return

