#!/usr/bin/python

"""
Brief:   This class sets generates all of the scripts needed for the ray-tracing
         phase and beyond, i.e. all of the per-chip scripts.

Date:    January 26, 2012
Authors: Nicole Silvestri, U. Washington, nms21@uw.edu,
         Jeff Gardner, U. Washington, Google, gardnerj@phys.washington.edu

Notation: For naming the rafts, sensors, amplifiers, and exposures, we
          obey the following convention:
             cid:    Chip/sensor ID string of the form 'R[0-4][0-4]_S[0-2][0-2]'
             ampid:  Amplifier ID string of the form 'C[0-1][0-7]'
             expid:  Exposure ID string of the form 'E[0-9][0-9][0-9]'
             id:     Full Exposure ID string of the form:
                              'R[0-4][0-4]_S[0-2][0-2]_E[0-9][0-9][0-9]'
             obshistid: ID of the observation from the trim file with the 'extraID'
                        digit appended ('clouds'=0, 'noclouds'=1).
"""

from __future__ import with_statement
import os, sys
import datetime
import glob
import math
import shutil
import subprocess
import string
import time

from SingleChipScriptGenerator import *
from chip import WithTimer
from chip import makeChipImage
from Exposure import filterToLetter
from Focalplane import *
#import lsst.pex.policy as pexPolicy
#import lsst.pex.logging as pexLog
#import lsst.pex.exceptions as pexExcept

def ReadExtraidFile(extraidFile):
    """Reads extraid and centid from extraid file

    Args:
      extraidFile:  extraid file pointer

    Returns:
      extraid, centid
      These can both be empty if not found.
    """
    extraid = ''
    centid = ''
    for line in extraidFile:
        if line.startswith('extraid'):
            name, next_extraid = line.split()
            print 'Found extraid:', next_extraid
            if next_extraid != '':
                extraid += next_extraid
        if line.startswith('centroidfile'):
            name, centid = line.split()
    return extraid, centid


def ReadObshistidAndFilt(trimfile):
    """Reads obshistid and filter number from trimfile.

    Args:
      trimfile:  trimfile file pointer

    Returns:
      obshistid, filterNum
    """
    filterNum = ''
    obshistid = ''
    for line in trimfile:
        if line.startswith('Opsim_filter'):
            name, filterNum = line.split()
        elif line.startswith('Opsim_obshistid'):
            name, obshistid = line.split()
    assert filterNum
    assert obshistid
    return obshistid, filterNum


class AllChipsScriptGenerator:
    """
    This class sets generates all of the scripts needed for the ray-tracing
    phase and beyond, i.e. all of the per-chip scripts.  It calls
    SingleChipScriptGenerator.makeScript() twice for each chip (2 exposures
    per chip) in loopOverChips().

    This is the class that is least changed from Nicole's original version,
    mostly because it has so few scheduler dependencies.  Similar to Nicole's version,
    you can call the class with a full exposure id and it will only run a single
    exposure instead of the full focal plane.
    """

    def __init__(self, trimfile, policy, extraidFile='', extraid='',
                 centid='0'):

        """
        (0) Initialize the full field instance catalog parameters and
        the opsim catalog parameters.  Gather all needed parameters
        from opsim and set up various header keywords and parameter
        file names.  Create necessary working and save directories.

        """

        self.policy = policy
        # Should not ever reference imsimSourcePath on exec node
        #self.imsimHomePath = os.getenv("IMSIM_SOURCE_PATH")
        #self.imsimDataPath = os.getenv("CAT_SHARE_DATA")

        self.workDir = os.getcwd()
        print 'fullFocalPlane being run in directory %s' %(self.workDir)
        #verbosity = 4
        #pexLog.Trace_setVerbosity('imsim', verbosity)
        trimfile = trimfile.strip()
        self.trimfile = trimfile

        #
        # Get necessary config file info
        #
        #self.policy = pexPolicy.Policy.createPolicy(imsimPolicy)
        #self.policy = ConfigParser.RawConfigParser()
        #self.policy.read(imsimPolicyFile)
        # Job params
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        #self.scratchPath = self.policy.get('general','scratchPath')
        self.scratchOutputDir = self.policy.get('general','scratchOutputDir')
        self.savePath = self.policy.get('general','savePath')
        self.stagePath = self.policy.get('general','stagePath1')
        self.stagePath2 = self.policy.get('general','stagePath2')
        self.useSharedSEDs = self.policy.getboolean('general','useSharedSEDs')
        self.debugLevel = self.policy.getint('general','debuglevel')
        self.regenAtmoscreens = self.policy.getboolean('general','regenAtmoscreens')

        # Sets self.obshistid, self.filterNum, self.extraid, self.centid:
        self._loadFocalplaneNames(extraidFile, extraid, centid)

        # SET USEFUL DIRECTORY PATHS

        """
        Make directories to store the logfiles and parameter files for
        each job.  Best practice on Minerva Cluster is to write log
        files (and data files) to a shared directory as opposed to
        writing them back to your home directory.

        These are the paths to your SAVE, LOGS, CENTROID, and PARAMETER
        directories.  This directory holds the log files for each job,
        a small gzipped file of common data for the visit, and a
        directory with the parameter files needed for each job.
        """
        visitID = '%s-f%s' %(self.obshistid, self.filterName)
        # Output files from the "visit" preprocessing stage are staged to visitSavePath
        self.stagePath2 = os.path.join(self.stagePath2, visitID)
        self.paramDir = os.path.join(self.stagePath2, 'run%s' %(self.obshistid))
        # The logs go into the savePath, however.
        self.logPath = os.path.join(self.savePath, visitID, "logs")
        # NOTE: This might not be in the right location, but I never ran with self.centid==1.
        self.centroidPath = os.path.join(self.stagePath, 'imSim/PT1.2/centroid/v%s-f%s' %(self.obshistid, self.filterName))

        self.focalplane = Focalplane(self.obshistid, self.filterName)
        _d = self.focalplane.parsDictionary
        # Parameter File Names
        self.obsCatFile        = _d['objectcatalog']
        self.obsParFile        = _d['obs']
        self.atmoParFile       = _d['atmosphere']
        self.atmoRaytraceFile  = _d['atmosphereraytrace']
        self.cloudRaytraceFile = _d['cloudraytrace']
        self.controlParFile    = _d['control']
        self.opticsParFile     = _d['optics']
        self.catListFile       = _d['catlist']
        self.trackingParFile   = _d['tracking']
        self.trackParFile      = _d['track']
        return

    def _loadFocalplaneNames(self, extraidFile, extraid, centid):
        """Reads obshistid, filter, extraid, centid from trimfile and extraidfile.

        Sets self.obshistid, self.filterName, self.filterNum, self.centid,
        and self.trimfile by reading self.trimfile and extraidFile.
        """
        #Get obshistid and filter ID from trimfile
        with file(self.trimfile, 'r') as trimf:
            self.obshistid, self.filterNum = ReadObshistidAndFilt(trimf)
        self.filterName = filterToLetter(self.filterNum)

        # Get non-default commands & extra ID
        self.centid = centid
        if extraidFile:
            self.extraidFile = extraidFile.strip()
            with file(self.extraidFile, 'r') as exFile:
                extraid, centid = ReadExtraidFile(exFile)
        else:
            self.extraidFile = ''
        self.extraid = extraid
        # Set obshistid to include extraid
        self.obshistid += self.extraid
        print 'extraid:', self.extraid
        print 'obshistid:', self.obshistid
        print 'Centroid FileID:', self.centid

    def makeScripts(self, idonly=""):
        """This is the main public method for this class.
        It generates all of the scripts for performing the raytracing
        phase on each chip (really, each exposure for each chip).
        It is comprised of the following 4 stages.
        """
        wav = self.focalplane.runPreprocessingCommands(trimfile=self.trimfile)
        self._setupScriptEnvironment(idonly)
        self._generateScripts(wav)
        self._stageAndCleanupFiles()
        return

    def _setupScriptEnvironment(self, idonly):
        """Configures the necessary variables and directories
        for generating the scripts for the raytracing phase.
        """
        self.idonly = idonly
        # Build the list of cids to process
        self.cidList = self.focalplane.generateCidList(idonly=self.idonly)
        self._makePaths()
        return

    def _makePaths(self):
        if not os.path.isdir(self.logPath):
          os.makedirs(self.logPath)
        print 'Your logfile directory is: ', self.logPath

        if not os.path.isdir(self.paramDir):
          os.makedirs(self.paramDir)
        print 'Your parameter staging directory is: ', self.paramDir

        if self.centid == '1':
            if not os.path.isdir(self.centroidPath):
                try:
                    os.makedirs(self.centroidPath)
                except OSError:
                    pass
            print 'Your centroid directory is %s' %(self.centroidPath)

        return

    def _generateScripts(self, wav):
        """Calls the proper SingleChipScriptGenerator class for each chip.
        INPUTS: wavelength returned from generateAtmosphericScreen()
        """
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator(self.policy, self.obshistid, self.filterName,
                                              self.filterNum, self.centid, self.centroidPath,
                                              self.stagePath2, self.paramDir,
                                              self.trackingParFile)
        self._loopOverChips(scriptGen, wav)
        return

    def _loopOverChips(self, scriptGen, wav):

        """
        For every chip in self.cidList, then for every exposure, generate
        the scripts for the raytrace, background, cosmic ray, and electron-to-ADC.
        For LSST Group1 CCDs, this is 378 single-chip exposures per focal plane.

        """
        parNames = ParsFilenames(self.obshistid)
        count = 1
        cc = 0
        for chip in self.cidList:
            cid = chip[0]
            trimCatFile = 'trimcatalog_%s_%s.pars' %(self.obshistid, cid)
            # The minimum length of trimCatFile at this stage is 2 lines.
            # (trim puts 1 line in there, and generateTrimCatalog() appended
            # a second line. Therefore, the number of sources in the trimCatFile
            # is its length - 2.
            with open(trimCatFile) as f:
                nTrimCatSources = len(f.readlines()) - 2
            print 'nTrimCatSources:', nTrimCatSources
            print 'minsource', self.focalplane.minsource
            if nTrimCatSources >= self.focalplane.minsource:

                # Figure out which SED files are needed for this chip and store
                # in sedlist_*.txt.  This information is useful for platforms
                # where we stage only the needed SED files.
                self.focalplane.writeSedManifest(trimCatFile, cid)
                # Now that we are done reading from trimCatFile, gzip it since
                # these files get rather large
                cmd = 'gzip %s' %trimCatFile
                sys.stdout.write('Gzipping %s...' %trimCatFile)
                subprocess.check_call(cmd, shell=True)
                sys.stdout.write('Done.\n')
                trimCatFile += '.gz'
                devtype = chip[1]
                devvalue = chip[2]
                if devtype == 'CCD':
                    nexp = self.focalplane.nsnap
                    exptime = (self.focalplane.vistime - (nexp-1) * devvalue) / nexp
                elif devtype == 'CMOS':
                    nexp = int(self.focalplane.vistime / devvalue)
                    exptime = self.focalplane.vistime / nexp
                else:
                    raise RuntimeError, "Unknown devtype=%s in focalplanelayout file:" %devtype
                print '# exposures (nexp):', nexp
                ex = 0
                while ex < nexp:
                    expid = 'E%03d' %ex
                    id = '%s_%s' %(cid, expid)
                    expTrimCatFile = parNames.trimcatalog(id)
                    shutil.copyfile(trimCatFile, expTrimCatFile)
                    timeParFile = parNames.time(expid)
                    if os.path.isfile(timeParFile):
                        os.remove(timeParFile)
                    if devtype == 'CCD':
                        timeoff = 0.5*exptime + ex*(devvalue + exptime) - 0.5*self.focalplane.vistime
                    elif devtype == 'CMOS':
                        timeoff = 0.5*exptime + ex*exptime - 0.5*self.focalplane.vistime
                    else:
                        raise RuntimeError, "Unknown devtype=%s in focalplanelayout file:" %devtype
                    with file(timeParFile, 'a') as parFile:
                        parFile.write('timeoffset %f \n' %timeoff)
                        parFile.write('pairid %d \n' %ex)
                        parFile.write('exptime %f \n' %exptime)
                    seedchip = int(self.focalplane.simseed) + cc*1000 + ex
                    cc += 1
                    chipParFile = parNames.chip(id)
                    if os.path.isfile(chipParFile):
                        os.remove(chipParFile)
                    shutil.copyfile('data/focal_plane/sta_misalignments/offsets/pars_%s' %(cid), chipParFile)
                    with file(chipParFile, 'a') as parFile:
                        #TODO: parFile.write('flatdir 1 \n')
                        parFile.write('chipid %s \n' %(cid))
                        parFile.write('chipheightfile ../data/focal_plane/sta_misalignments/height_maps/%s.fits.gz \n' %(cid))
                    # GENERATE THE RAYTRACE PARS
                    print 'Generating raytrace pars.'
                    raytraceParFile = parNames.raytrace(id)
                    self.focalplane.generateRaytraceParams(id, chipParFile, seedchip, timeParFile,
                                                           raytraceParFile, self.extraidFile)
                    # GENERATE THE BACKGROUND ADDER PARS
                    print 'Generating background pars.'
                    backgroundParFile = parNames.background(id)
                    self.focalplane.generateBackgroundParams(id, seedchip, cid, wav, backgroundParFile)
                    # GENERATE THE COSMIC RAY ADDER PARS
                    print 'Generating cosmic rays pars'
                    cosmicParFile = parNames.cosmic(id)
                    self.focalplane.generateCosmicRayParams(id, seedchip, exptime, cosmicParFile)
                    # GENERATE THE ELECTRON TO ADC CONVERTER PARS
                    print 'Generating e2adc pars.'
                    self.focalplane.generateE2adcParams(id, cid, expid, seedchip, exptime)

                    if self.idonly:
                        try:
                            os.mkdir(self.scratchOutputDir)
                        except:
                            print 'WARNING: Directory %s already exists!' %(self.scratchOutputDir)
                            pass
                        makeChipImage(self.obshistid, self.filterNum, cid, expid, self.scratchOutputPath)
                    else:
                        # MAKE THE SINGLE-CHIP SCRIPTS
                        print 'Making Single-Chip Scripts.'
                        scriptGen.makeScript(cid, expid, raytraceParFile, backgroundParFile,
                                             cosmicParFile, expTrimCatFile, self.logPath,
                                             self.trimfile)
                    print 'Count:', count
                    count += 1
                    ex += 1
            os.remove(trimCatFile)  # if nTrimCatSources >= self.minsource:
        return


    def _stageAndCleanupFiles(self, nodeFilesBasename='nodeFiles'):
        """

        Make tarball of needed files for local nodes, create directories
        on shared file system for logs and images (if not copying to IRODS
        storage on Pogo1) and cleanup home directory by moving all files
        to 'run' directory.

        """
        os.chdir('%s' %(self.workDir))
        if not self.idonly:
            # Telescope data and parameter files (and data files if needed)
            nodeFilesTar = '%s%s.tar' % (nodeFilesBasename, self.obshistid)
            self._tarTelescopeParamFiles(nodeFilesTar)
            self._tarParsFiles(nodeFilesTar)
            # Executables and binaries files for running on nodes.
            nodeFilesExecTar = '%sExec%s.tar' % (nodeFilesBasename, self.obshistid)
            self._tarExecFiles(nodeFilesExecTar)
            if self.regenAtmoscreens:
                self._tarRegenAtmoscreenFiles(nodeFilesTar, nodeFilesExecTar)
            else:
                self._tarAtmosphereFiles(nodeFilesTar)
            # Zip the tar files.
            print 'Gzipping %s' % nodeFilesTar
            cmd = 'gzip %s' % nodeFilesTar
            subprocess.check_call(cmd, shell=True)
            nodeFilesTar += '.gz'
            print 'Gzipping %s' % nodeFilesExecTar
            cmd = 'gzip %s' % nodeFilesExecTar
            subprocess.check_call(cmd, shell=True)
            nodeFilesExecTar += '.gz'
            # Move to stagePath2
            self._stageNodeFilesTarball(nodeFilesTar)
            self._stageNodeFilesTarball(nodeFilesExecTar)
        # Move the parameter, and fits files to the run directory.
        self._cleanupFitsFiles()
        self._cleanupParFiles()
        self._cleanupSedFiles()
        # Move the script files if created (i.e. not in single-chip mode)
        if not self.idonly:
            self._cleanupScriptFiles()
        return

    def _tarAtmosphereFiles(self, nodeFilesTar, tarCommand='rf'):
        # Files needed for ancillary/atmosphere
        print 'Tarring atmosphere files.'
        cmd =  'tar %s %s' % (tarCommand, nodeFilesTar)
        cmd += ' atmospherescreen_%s_*.fits cloudscreen_%s_*.fits' %(self.obshistid,
                                                                     self.obshistid)
        subprocess.check_call(cmd, shell=True)

    def _tarParsFiles(self, nodeFilesTar, tarCommand='rf'):
        print 'Tarring Pars files.'
        cmd = 'tar %s %s %s %s %s' % (tarCommand, nodeFilesTar, self.cloudRaytraceFile,
                                      self.controlParFile, self.obsCatFile)
        subprocess.check_call(cmd, shell=True)

    def _tarExecFiles(self, nodeFilesTar, tarCommand='cf'):
        print 'Tarring binaries/executables.'
        cmd = ('tar %s %s ancillary/trim/trim ancillary/Add_Background/*'
               ' ancillary/cosmic_rays/* ancillary/e2adc/e2adc raytrace/lsst'
               ' raytrace/*.txt raytrace/version pbs/distributeFiles.py'
               ' Exposure.py Focalplane.py verifyFiles.py chip.py'
               % (tarCommand, nodeFilesTar))
        subprocess.check_call(cmd, shell=True)
        return

    def _tarTelescopeParamFiles(self, nodeFilesTar, telescope='lsst', tarCommand='cf'):
        print 'Tarring telescope param files in %s.' % telescope
        fileGlob = os.path.join(telescope, '*.txt')
        cmd =  'tar %s %s %s' % (tarCommand, nodeFilesTar, fileGlob)
        subprocess.check_call(cmd, shell=True)

    def _tarRegenAtmoscreenFiles(self, nodeFilesTar, nodeFilesExecTar, tarCommand='rhf'):
      print 'Tarring atmosphere screen regeneration binaries.'
      cmd = ('tar %s %s ancillary/atmosphere_parameters/* ancillary/atmosphere/*'
             % (tarCommand, nodeFilesExecTar))
      subprocess.check_call(cmd, shell=True)
      print 'Tarring atmosphere screen regeneration data files'
      if os.path.dirname(self.trimfile):
        # If the trimfile exists elsewhere on the filesystem, there should be
        # a symbolic link in the cwd.
        assert os.path.islink(os.path.basename(self.trimfile))
      cmd = 'tar %s %s default_instcat %s' % (tarCommand, nodeFilesTar,
                                              os.path.basename(self.trimfile))
      subprocess.check_call(cmd, shell=True)

    def _stageNodeFilesTarball(self, nodeFilesTar):
        print 'Moving %s to %s/.' %(nodeFilesTar, self.stagePath2)
        if os.path.isfile(os.path.join(self.stagePath2, nodeFilesTar)):
            try:
                os.remove(os.path.join(self.stagePath2, nodeFilesTar))
            except OSError:
                pass
        shutil.move(nodeFilesTar, '%s/' %(self.stagePath2))

    def _cleanupFitsFiles(self):
        #print 'Moving FITS files to %s.' %(self.paramDir)
        print 'Deleting FITS files'
        for fits in glob.glob('*.fits'):
            # Using copyfile instead of move will overwrite the destination if already present
            #shutil.copy(fits, '%s' %(self.paramDir))
            os.remove(fits)
        return

    def _cleanupParFiles(self):
        print 'Moving .par and .par.gz files to %s.' %(self.paramDir)
        for pars in (glob.glob('*.pars')+glob.glob('*.pars.gz')):
            shutil.copy(pars, '%s' %(self.paramDir))
            os.remove(pars)
        return

    def _cleanupSedFiles(self):
        print 'Moving sedlist_*.txt files to %s.' %(self.paramDir)
        for seds in glob.glob('sedlist_*.txt'):
            shutil.copy(seds, '%s' %(self.paramDir))
            os.remove(seds)
        return

    def _cleanupScriptFiles(self):
        scriptListFilename = generateRaytraceJobManifestFilename(self.obshistid, self.filterName)
        if os.path.isfile(scriptListFilename):
            os.remove(scriptListFilename)
        # Deal with the script and command files if created (not single chip mode).
        print 'Moving shell script files to', self.paramDir
        print 'And generating list of scripts in', scriptListFilename
        with open(scriptListFilename, 'w') as scriptList:
            for exec_filename in sorted(glob.glob('exec_%s_*.csh' %(self.obshistid))):
                shutil.copy(exec_filename, '%s' %(self.paramDir))
                os.remove(exec_filename)
                cmd = 'csh'
                if self.debugLevel > 0:
                  cmd += ' -x'
                scriptList.write('%s %s\n' %(cmd, os.path.join(self.paramDir, exec_filename)))

        try:
            for cmds in glob.glob('cmds_*.txt'):
                os.remove(cmds)
        except:
            print 'WARNING: No command files to remove!'
            pass
        return



class AllChipsScriptGenerator_Pbs(AllChipsScriptGenerator):

    def __init__(self, trimfile, policy, extraidFile='', extraid='0',
                 centid='0'):
        AllChipsScriptGenerator.__init__(self, trimfile, policy, extraidFile,
                                         extraid, centid)
        self.username = self.policy.get('pbs','username')
        print 'Your PBS username is: ', self.username
        return

    def _generateScripts(self, wav):
        """Calls the proper SingleChipScriptGenerator class for each chip.
        INPUTS: wavelength returned from generateAtmosphericScreen()
        """
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator_Pbs(self.policy, self.obshistid, self.filterName,
                                                  self.filterNum, self.centid, self.centroidPath,
                                                  self.stagePath2, self.paramDir,
                                                  self.trackingParFile)
        self._loopOverChips(scriptGen, wav)
        return

    def _cleanupScriptFiles(self):
        scriptListFilename = generateRaytraceJobManifestFilename(self.obshistid, self.filterName)
        if os.path.isfile(scriptListFilename):
            os.remove(scriptListFilename)
        # Deal with the pbs and command files if created (not single chip mode).
        print 'Moving PBS script files to', self.paramDir
        with open(scriptListFilename, 'w') as scriptList:
            for pbs in sorted(glob.glob('exec_%s_*.pbs' %(self.obshistid))):
                shutil.copy(pbs, '%s' %(self.paramDir))
                os.remove(pbs)
                scriptList.write('qsub %s\n' %os.path.join(self.paramDir, pbs))

        try:
            for cmds in glob.glob('cmds_*.txt'):
                print 'Removing %s' %(cmds)
                os.remove(cmds)
        except:
            print 'WARNING: No command files to remove!'
            pass
