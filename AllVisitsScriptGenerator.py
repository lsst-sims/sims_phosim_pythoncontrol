#!/usr/bin/python

"""
Brief:   Main classes for generating the the shell scripts for all of
         the visits/trimfiles. Calls methods from SingleVisitScriptGenerator.
         See classes for more info.

Authors: Nicole Silvestri, U. Washington, nms@astro.washington.edu
         Jeffrey P. Gardner, U. Washington, gardnerj@phys.washington.edu

"""

from __future__ import with_statement
import os
import subprocess
import shutil
import tempfile

#import lsst.pex.policy as pexPolicy
import ConfigParser
import getpass   # for getting username
import datetime
from SingleVisitScriptGenerator import *
from Exposure import filterToLetter, filterToNumber

class AllVisitsScriptGenerator:
    """
    Main class for generating the shell scripts for all of the visits/trimfiles
    that have been submitted.  Calls methods from SingleVisitScriptGenerator.
    This can be used as a superclass for PBS- or Exacycle-specific script
    generators.

    'Public' methods:
        - makeScripts()
    """

    def __init__(self, myfile, policy, imsimConfigFile, extraIdFile):

        self.policy = policy

        # Create a temporary directory in which to assemble tar files
        self.tmpdir = tempfile.mkdtemp()
        print 'self.tmpdir:', self.tmpdir

        # We want to track the path where the script was originally
        # invoked because we will have to cd to other directories
        # temporarily.
        self.scriptInvocationPath = os.getcwd()
        self._loadEnvironmentVars()
        self.imsimConfigFile = imsimConfigFile

        #policy   = pexPolicy.Policy.createPolicy(imsimPolicy)
        # Job params
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        #self.scratchPath = self.policy.get('general','scratchExecPath')
        self.savePath  = self.policy.get('general','savePath')
        self.stagePath  = self.policy.get('general','stagePath1')
        self.stagePath2  = self.policy.get('general','stagePath2')
        # Job monitor database
        self.useDatabase = self.policy.getboolean('general','useDatabase')

        #
        # Load list of trimfiles
        #
        self.trimfileList = self._loadTrimfileList(myfile)

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
                        #Note that this is a string addition operator
                        self.extraid = self.extraid + extraid_tmp
        print 'Final extraid:', self.extraid


    def __del__(self):
      # We are responsible for removing the temp dir
      shutil.rmtree(self.tmpdir)
      return

    def _loadEnvironmentVars(self):
        self.imsimSourcePath = os.getenv("IMSIM_SOURCE_PATH")
        if self.imsimSourcePath is None:
            raise NameError('Could not find value for IMSIM_SOURCE_PATH.')
        self.imsimExecPath = os.getenv("IMSIM_EXEC_PATH")
        if self.imsimExecPath == None:
            self.imsimExecPath = self.imsimSourcePath
        #self.imsimDataPath = os.getenv("CAT_SHARE_DATA")
        #if self.imsimDataPath is None:
        #    raise NameError('Could not find value for CAT_SHARE_DATA.')
        return

    def _loadTrimfileList(self, myfile):
        myFiles = '%s' %(myfile)
        # files = open(myFiles).readlines()
        return open(myFiles).readlines()


    def makeScripts(self):
        """
        This is the only method designed to be public.
        Loops over trimfiles in trimfileList and calls processTrimFile which reads
        in the trimfile and then calls scriptGen.makeScript() to generate the actual
        script.
        """
        preprocScriptManifest = os.path.join(self.stagePath, 'preprocessingJobs.lis')
        self.checkDirectories(preprocScriptManifest)

        # Note that tarExecFiles() needs to be called before initializing
        # SingleVisitScriptGenerator because it defines self.execFileTgzName
        # Same with tarSourceFiles() and tarControlFiles().
        self.tarSourceFiles()
        self.tarExecFiles()
        self.tarControlFiles()
        # SingleVisitScriptGenerator can be instantiated only once per execution environment,
        # So initialize it here, then call the makeScript() in the loop over trim files.
        scriptGen = SingleVisitScriptGenerator(self.scriptInvocationPath, preprocScriptManifest,
                                               self.policy, self.imsimConfigFile, self.extraIdFile,
                                               self.sourceFileTgzName, self.execFileTgzName,
                                               self.controlFileTgzName, self.tmpdir)

        visitDirList = []
        for trimfileName in self.trimfileList:
            trimfileName = trimfileName.strip()
            visitDirList.append(self.processTrimFile(scriptGen, trimfileName))
        return visitDirList


    def processTrimFile(self, scriptGen, trimfileName):
        """
        Process a single trimfile.
           scriptGen:      instance of SingleVisitScriptGenerator
           trimfileName:   name of the trimfile in question
        """
        # trimfileName is the fully-qualified name of the trimfile we are processing
        trimfileBasename = os.path.basename(trimfileName)
        trimfilePath = os.path.dirname(trimfileName)
        #basename, extension = os.path.splitext(trimfileName)

        for line in open(trimfileName).readlines():
            if line.startswith('Opsim_filter'):
                name, filterNum = line.split()
                print 'Opsim_filter:', filterNum
            if line.startswith('Opsim_obshistid'):
                name, obshistid = line.split()
                print 'Opsim_obshistid:', obshistid

        ono = list(obshistid)
        if len(ono) > 8:
            origObshistid = '%s%s%s%s%s%s%s%s' %(ono[0], ono[1], ono[2], ono[3],
                                                 ono[4],ono[5], ono[6], ono[7])
        else:
            origObshistid = obshistid

        # Add in extraid
        obshistid = obshistid + self.extraid

        # Create SAVE & LOG DIRECTORIES for this visit
        filterName = filterToLetter(filterNum)
        visitDir = '%s-f%s' %(obshistid, filterName)
        visitSavePath = os.path.join(self.stagePath2, visitDir)
        visitLogPath = os.path.join(self.savePath, visitDir, 'logs')
        visitParamDir = 'run%s' %(obshistid)  # Subdirectory within visitSavePath to store param files
        trimfileStagePath = os.path.join(self.stagePath, 'trimfiles', visitDir)

        self.checkVisitDirectories(visitSavePath, visitLogPath, visitParamDir, trimfileStagePath)
        scriptGen.makeScript(obshistid, origObshistid, trimfileName, trimfileBasename,
                             trimfilePath, filterName, filterNum, visitDir, visitLogPath)
        return visitDir

    def checkDirectories(self, preprocScriptManifest):
        # Checks directories accessible from the client that are used for all visits
        stagePath = self.stagePath
        # Creat stagePath if it does not exist
        if not os.path.isdir(stagePath):
            try:
                print 'Creating %s' %(stagePath)
                os.makedirs(stagePath)
            except OSError:
                print OSError
        # Remove the file containing the script names if it exists.
        if os.path.isfile(preprocScriptManifest):
          os.remove(preprocScriptManifest)
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
        return

    def tarSourceFiles(self):
        """
        Tar all the files in the source tree that are not executables.
        None of the files in the source tree should be visit-dependent.
        """
        self.sourceFileTgzName = 'imsimSourceFiles.tar.gz'
        os.chdir(self.imsimSourcePath)
        cmd =  'tar czf %s' % os.path.join(self.tmpdir, self.sourceFileTgzName)
        cmd += ' lsst/*.txt ancillary/atmosphere_parameters/*.txt'
        cmd += ' ancillary/Add_Background/filter_constants* ancillary/Add_Background/fits_files'
        cmd += ' ancillary/Add_Background/SEDs/*.txt ancillary/Add_Background/vignetting_*.txt'
        cmd += ' ancillary/cosmic_rays/iray_textfiles/iray* raytrace/*.txt'
        cmd += ' raytrace/version pbs/distributeFiles.py *_instcat'
        print 'Tarring all source files.'
        subprocess.check_call(cmd, shell=True)
        # cd back to the invocation directory
        os.chdir(self.scriptInvocationPath)
        return


    def tarExecFiles(self):
        """
        Tar all the ImSim exec files.  We do this separately from the
        rest of the source tree to handle the case where the compiled exec
        files don't end up in the same location.
        NOTE TO SELF: Possibly use random file name to avoid collision
                      with other script invocations.
        """
        self.execFileTgzName = 'imsimExecFiles.tar.gz'
        os.chdir(self.imsimExecPath)
        # Explicitly packaged
        cmd = 'tar czf %s ancillary/atmosphere_parameters/create_atmosphere ancillary/atmosphere/cloud ancillary/atmosphere/turb2d ancillary/optics_parameters/optics_parameters ancillary/trim/trim ancillary/Add_Background/add_background ancillary/Add_Background/update_filter_constants ancillary/cosmic_rays/create_rays ancillary/e2adc/e2adc ancillary/tracking/tracking raytrace/lsst' % os.path.join(self.tmpdir, self.execFileTgzName)
        print 'Tarring all exec files.'
        subprocess.check_call(cmd, shell=True)
        os.chdir(self.scriptInvocationPath)
        return


    def tarControlFiles(self):
        """
        Make tarball of the control scripts and param files needed on the exec nodes.
        """
        self.controlFileTgzName = 'imsimControlFiles.tar.gz'
        # We should be in the script's invocation directory
        assert os.getcwd() == self.scriptInvocationPath

        cmd =  'tar czvf %s ' % os.path.join(self.tmpdir, self.controlFileTgzName)
        cmd += ' chip.py fullFocalplane.py AbstractScriptGenerator.py AllChipsScriptGenerator.py'
        cmd += ' SingleChipScriptGenerator.py Focalplane.py Exposure.py verifyFiles.py %s %s' %(self.imsimConfigFile, self.extraIdFile)

        print 'Tarring control and param files that will be copied to the execution node(s).'
        subprocess.check_call(cmd, shell=True)

        return


class AllVisitsScriptGenerator_Pbs(AllVisitsScriptGenerator):
    """
    This class redefines scriptWriter() for PBS.
    """

    def __init__(self, myfile, policy, imsimConfigFile, extraIdFile):
        """
        Augmentation to the superclass's constructor to get PBS-specific 'username'.
        """
        AllVisitsScriptGenerator.__init__(self, myfile, policy, imsimConfigFile, extraIdFile)
        # Check to make sure we are the correct class for the "scheduler1" option
        assert self.policy.get('general','scheduler1') == 'pbs'
        self.username = self.policy.get('pbs','username')
        # Redefine scratchPath to include username.
        #self.scratchPath = os.path.join(self.policy.get('general','scratchPath'), username)


    def makeScripts(self):
        """
        Loops over trimfiles in trimfileList and calls processTrimFile which reads
        in the trimfile and then calls scriptGen.makeScript() to generate the actual
        script.
        """
        preprocScriptManifest = os.path.join(self.stagePath, 'preprocessingJobs.lis')
        self.checkDirectories(preprocScriptManifest)

        # Note that tarExecFiles() needs to be called before initializing
        # SingleVisitScriptGenerator because it defines self.execFileTgzName
        # Same with tarSourceFiles() and tarControlFiles().
        self.tarSourceFiles()
        self.tarExecFiles()
        self.tarControlFiles()
        # SingleVisitScriptGenerator can be instantiated only once per execution environment,
        # So initialize it here, then call the makeScript() in the loop over trim files.
        scriptGen = SingleVisitScriptGenerator_Pbs(self.scriptInvocationPath, preprocScriptManifest,
                                                   self.policy, self.imsimConfigFile,
                                                   self.extraIdFile, self.sourceFileTgzName,
                                                   self.execFileTgzName, self.controlFileTgzName,
                                                   self.tmpdir)

        visitDirList = []
        for trimfileName in self.trimfileList:
            trimfileName = trimfileName.strip()
            visitDirList.append(self.processTrimFile(scriptGen, trimfileName))
        return
