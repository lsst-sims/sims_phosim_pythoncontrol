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
from Focalplane import *
#import lsst.pex.policy as pexPolicy
#import lsst.pex.logging as pexLog
#import lsst.pex.exceptions as pexExcept

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

    def __init__(self, trimfile, policy, extraidFile):

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

        #
        # LSST-specific params
        #
        self._readTrimfilesAndCalculateParams()

        # Get non-default commands & extra ID
        self.centid = '0'
        self.extraidFile = extraidFile.strip()
        if self.extraidFile != '':
            for line in open(self.extraidFile).readlines():
                if line.startswith('extraid'):
                    name, self.extraid = line.split()
                    print 'extraid:', self.extraid
                    if self.extraid != '':
                        self.obshistid = self.obshistid+self.extraid
                if line.startswith('centroidfile'):
                    name, self.centid = line.split()
        print 'Centroid FileID:', self.centid

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

        filtmap = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
        self.filter = filtmap[self.filt]
        visitID = '%s-f%s' %(self.obshistid, self.filter)
        # Output files from the "visit" preprocessing stage are staged to visitSavePath
        self.stagePath2 = os.path.join(self.stagePath2, visitID)
        self.paramDir = os.path.join(self.stagePath2, 'run%s' %(self.obshistid))
        # The logs go into the savePath, however.
        self.logPath = os.path.join(self.savePath, visitID, "logs")
        # NOTE: This might not be in the right location, but I never ran with self.centid==1.
        self.centroidPath = os.path.join(self.stagePath, 'imSim/PT1.2/centroid/v%s-f%s' %(self.obshistid, self.filter))

        self.Focalplane = Focalplane(self.obshistid, self.filter)
        _d = self.Focalplane.parsDictionary
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

    def _readTrimfilesAndCalculateParams(self):
        # Read in the default catalog first, then replace the non-default
        # values with the actual trimfile
        print 'Using instance catalog: default_instcat',
        print '***'
        with open('default_instcat','r') as trimfile:
            self._readTrimfile(trimfile)
        print 'Using instance catalog: ', self.trimfile
        print '***'
        with open(self.trimfile,'r') as trimfile:
            self._readTrimfile(trimfile)
        self._calculateParams()
        return

    def _readTrimfile(self, trimfile):
        print 'Initializing Opsim and Instance Catalog Parameters.'
        for line in trimfile.readlines():
            if line.startswith('SIM_SEED'):
                name, self.obsid = line.split()
                print 'SIM_SEED:', self.obsid
            if line.startswith('Unrefracted_RA'):
                name, self.pra = line.split()
                print 'Unrefracted_RA:', self.pra
            if line.startswith('Unrefracted_Dec'):
                name, self.pdec = line.split()
                print 'Unrefracted_Dec:', self.pdec
            if line.startswith('Opsim_moonra'):
                name, self.mra = line.split()
                print 'Opsim_moonra:', self.mra
            if line.startswith('Opsim_moondec'):
                name, self.mdec = line.split()
                print 'Opsim_moondec:', self.mdec
            if line.startswith('Opsim_rotskypos'):
                name, self.prot = line.split()
                print 'Opsim_rotskypos:', self.prot
            if line.startswith('Opsim_rottelpos'):
                name, self.spid = line.split()
                print 'Opsim_rottelpos:', self.spid
            if line.startswith('Opsim_filter'):
                name, self.filt = line.split()
                print 'Opsim_filter:', self.filt
            if line.startswith('Unrefracted_Altitude'):
                name, self.alt = line.split()
                print 'Unrefracted_Altitude:', self.alt
            if line.startswith('Unrefracted_Azimuth'):
                name, self.az = line.split()
                print 'Unrefracted_Azimuth:', self.az
            if line.startswith('Opsim_rawseeing'):
                name, self.rawseeing = line.split()
                print 'Opsim_rawseeing:', self.rawseeing
            if line.startswith('Opsim_sunalt'):
                name, self.sunalt = line.split()
                print 'Opsim_sunalt:', self.sunalt
            if line.startswith('Opsim_moonalt'):
                name, self.moonalt = line.split()
                print 'Opsim_moonalt:', self.moonalt
            if line.startswith('Opsim_dist2moon'):
                name, self.moondist = line.split()
                print 'Opsim_dist2moon:', self.moondist
            if line.startswith('Opsim_moonphase'):
                name, self.moonphase = line.split()
                print 'Opsim_moonphase:', self.moonphase
            if line.startswith('Slalib_date'):
                name, stringDate = line.split()
                year, self.month, day, time = stringDate.split('/')
                print 'Slalib_date:', self.month
            if line.startswith('Opsim_obshistid'):
                name, self.obshistid = line.split()
                print 'Opsim_obshistid: ', self.obshistid
            if line.startswith('Opsim_expmjd'):
                name, self.tai = line.split()
                print 'Opsim_expmjd: ', self.tai
            if line.startswith('SIM_MINSOURCE'):
                name, self.minsource = line.split()
                # if SIM_MINSOURCE = 0 - images will be generated for
                # chips with zero stars on them (background images)
                print 'Sim_Minsource: ', self.minsource
            if line.startswith('SIM_TELCONFIG'):
                name, self.telconfig = line.split()
                print 'Sim_Telconfig: ', self.telconfig
            if line.startswith('SIM_CAMCONFIG'):
                self.camconfig = int(line.split()[1])
                print 'Sim_Camconfig: ', self.camconfig
            if line.startswith('SIM_VISTIME'):
                self.vistime = float(line.split()[1])
                print 'Sim_Vistime: ', self.vistime
            if line.startswith('SIM_NSNAP'):
                self.nsnap = int(line.split()[1])
                print 'Sim_Nsnap: ', self.nsnap
            if line.startswith('isDithered'):
                self.isDithered = int(line.split()[1])
                print 'isDithered: ', self.isDithered
            if line.startswith('ditherRaOffset'):
                self.ditherRaOffset = float(line.split()[1])
                print 'ditherRaOffset: ', self.ditherRaOffset
            if line.startswith('ditherDecOffset'):
                self.ditherDecOffset = float(line.split()[1])
                print 'ditherDecOffset: ', self.ditherDecOffset
        return

    def _calculateParams(self):
        # Calculated Parameters
        tempDate = datetime.date.today()
        sDate = str(tempDate)
        year, mo, day = sDate.split('-')

        self.readtime = 3.0
        print 'Readtime:', self.readtime
        # These are now calculated per chip in loopOverChips()
        #self.exptime = 0.5*(float(self.vistime)) - 0.5*(float(self.readtime))
        #print 'Exptime:', self.exptime
        #self.timeoff = 0.5*(float(self.exptime)) + 0.5*(float(self.readtime))
        #print 'Timeoff:', self.timeoff
        self.starttime = -0.5*(self.vistime)
        print 'StartTime:', self.starttime
        self.endtime = 0.5*(self.vistime)
        print 'EndTime:', self.endtime
        self.moonphaserad = 3.14159 - 3.14159*(float(self.moonphase))/100.0
        print 'MoonPhase Radians:', self. moonphaserad
        self.sigmarawseeing = float(self.rawseeing)/2.35482
        print 'Sigma RawSeeing:', self.sigmarawseeing
        self.azim = float(self.az)
        print 'Azimuth:', self.azim
        self.zen = 90 - float(self.alt)
        print 'Zenith:', self.zen
        self.rrate = 15.04*math.cos(30.66*3.14159/180)*math.cos(float(self.az)*3.14159/180)/math.cos(float(self.alt)*3.14159/180)
        print 'RRate:', self.rrate
        self.sunzen = 90 - float(self.sunalt)
        print 'Sun Zenith:', self.sunzen
       	self.minsource = int(self.minsource)
        self.ncat = 0
        if not self.camconfig:
            raise RuntimeError, "SIM_CAMCONFIG was not supplied."
        if self.camconfig == 1:
            self.camstr = 'Group0'
        elif self.camconfig == 2:
            self.camstr = 'Group1'
        elif self.camconfig == 3:
            self.camstr = 'Group0|Group1'
        elif self.camconfig == 4:
            self.camstr = 'Group2'
        elif self.camconfig == 5:
            self.camstr = 'Group0|Group2'
        elif self.camconfig == 6:
            self.camstr = 'Group1|Group2'
        elif self.camconfig == 7:
            self.camstr = 'Group0|Group1|Group2'
        else:
            raise RuntimeError, "SIM_CAMCONFIG=%d is not valid." % self.camconfig
        return


    def makeScripts(self, extraidFile, idonly=""):
        """This is the main public method for this class.
        It generates all of the scripts for performing the raytracing
        phase on each chip (really, each exposure for each chip).
        It is comprised of the following 4 stages.
        """
        self._setupScriptEnvironment(extraidFile, idonly)
        wav = self._runPreprocessingCommands()
        self._generateScripts(wav)
        self._stageAndCleanupFiles()
        return
        
    def _setupScriptEnvironment(self, extraidFile, idonly):
        """Configures the necessary variables and directories
        for generating the scripts for the raytracing phase.
        """
        self.idonly = idonly
        # Build the list of cids to process
        self.cidList = self.Focalplane.generateCidList(self.camstr, self.idonly)
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

    def _runPreprocessingCommands(self):

        """
        This is the main worker routine.  It just goes through and calls
        all of Nicole's original functions.
        """
        self.writeObsCatParams()
        self.generateAtmosphericParams()
        wav = self.generateAtmosphericScreen()
        self.generateCloudScreen()
        self.generateControlParams()
        self.generateTrackingParams()
        self.generateTrimCatalog()
        return wav

    def _generateScripts(self, wav):
        """Calls the proper SingleChipScriptGenerator class for each chip.
        INPUTS: wavelength returned from generateAtmosphericScreen()
        """
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator(self.policy, self.obshistid, self.filter,
                                              self.filt, self.centid, self.centroidPath,
                                              self.stagePath2, self.paramDir,
                                              self.trackingParFile)
        self._loopOverChips(scriptGen, wav)
        return

    def writeObsCatParams(self):

        """

        (1) Write the parameters from the object catalog file to the
        obs parameter file.  This section is referred to as
        'Constructing a Catalog List' in John's full_focalplane shell
        script, most of which is new for PT1.2.

        """

        print 'Writing the ObsCat Parameters File.'
        objectTestFile = 'objecttest_%s.pars' %(self.obshistid)
        includeObjFile = 'includeobj_%s.pars' %(self.obshistid)

        if os.path.isfile(self.catListFile):
            try:
                os.remove(self.catListFile)
            except OSError:
                pass
        if os.path.isfile(self.obsCatFile):
            try:
                os.remove(self.obsCatFile)
            except OSError:
                pass
        if os.path.isfile(objectTestFile):
            try:
                os.remove(objectTestFile)
            except OSError:
                pass

        with file(objectTestFile, 'a') as parFile:
            parFile.write('object \n')

        cmd = 'grep object %s %s >> %s' %(self.trimfile, objectTestFile, self.obsCatFile)
        subprocess.check_call(cmd, shell=True)

        numlines = len(open(self.obsCatFile).readlines())
        print 'numlines', numlines
        if numlines > 1:
            try:
                os.remove(self.obsCatFile)
            except:
                #print 'WARNING: No file %s to remove!' %(self.obsCatFile)
                pass
            cmd = 'grep object %s >> %s' %(self.trimfile, self.obsCatFile)
            subprocess.check_call(cmd, shell=True)

            with file(self.catListFile, 'a') as parFile:
                parFile.write('catalog %s ../../%s \n' %(self.ncat, self.obsCatFile))
            self.ncat = 1

        try:
            os.remove(includeObjFile)
        except:
            #print 'WARNING: No file %s to remove!' %(includeObjFile)
            pass

        with file(includeObjFile, 'a') as parFile:
            parFile.write('includeobj \n')

        cmd = ('grep includeobj %s %s' %(self.trimfile, includeObjFile))
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        nincobj = len(results)

        if nincobj > 1:
            cmd = ('grep includeobj %s | awk -v x=%i \'{print "catalog",x,"../../"$2};{x+=1}\' >> %s' %(self.trimfile, self.ncat, self.catListFile))
            subprocess.check_call(cmd, shell=True)
            self.ncat = len(open(self.catListFile).readlines())

        try:
            os.remove(includeObjFile)
        except:
            #print 'WARNING: No file %s to remove - final!' %(includeObjFile)
            pass

        try:
            os.remove(objectTestFile)
        except:
            #print 'WARNING: No file %s to remove - final!' %(objectTestFile)
            pass

        try:
            os.remove(self.obsParFile)
        except:
            #print 'WARNING: No file %s to remove - final!' %(self.obsParFile)
            pass

        with file(self.obsParFile, 'a') as parFile:
            parFile.write('pointingra %s \n' %(self.pra))
            parFile.write('pointingdec %s \n' %(self.pdec))
            parFile.write('rotationangle %s \n' %(self.prot))
            parFile.write('spiderangle %s \n' %(self.spid))
            parFile.write('filter %s \n' %(self.filt))
            parFile.write('zenith %s \n' %(self.zen))
            parFile.write('azimuth %s \n' %(self.azim))
            parFile.write('rotationrate %s \n' %(self.rrate))
            parFile.write('seddir ../data/ \n')
            parFile.write('obshistid %s \n' %(self.obshistid))
            parFile.write('tai %s \n' %(self.tai))
            parFile.write('dithered %s\n' %self.isDithered)
            parFile.write('ditherra %s\n' %self.ditherRaOffset)
            parFile.write('ditherdec %s\n' %self.ditherDecOffset)

        return

    def generateAtmosphericParams(self):

        """
        (2) Create the files containing the atmosphere parameters.
        """

        print 'Writing the AtmosCat Parameters File.'

        try:
            os.remove(self.atmoParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(self.atmoParFile)
            pass

        with file(self.atmoParFile, 'a') as parFile:
            parFile.write('outputfilename %s \n' %(self.atmoRaytraceFile))
            parFile.write('monthnum %s \n' %(self.month))
            parFile.write('numlevel 6 \n')
            parFile.write('constrainseeing %s \n' %(self.sigmarawseeing))
            parFile.write('seed %s \n'%(self.obsid))
            parFile.write('createatmosphere\n')

        os.chdir('ancillary/atmosphere_parameters')
        cmd = 'time ./create_atmosphere < ../../%s' %(self.atmoParFile)
        sys.stderr.write('Running: %s\n'% cmd)
        with WithTimer() as t:
          subprocess.check_call(cmd, shell=True)
        t.PrintWall('create_atmosphere', sys.stderr)
        # Do a copy then remove, since shutil.copy() overwrites.
        shutil.copy('%s' %(self.atmoRaytraceFile), '../../')
        os.remove(self.atmoRaytraceFile)
        os.chdir('../../')

        return

    def generateAtmosphericScreen(self):

        """
        (3) Create the atmosphere screens.
        """

        print 'Generating the Atmospheric Screens.'

        os.chdir('ancillary/atmosphere')
        screenNumber = [0,1,2,3,4,5,6]
        for screen in screenNumber:
            for line in open('../../%s' %(self.atmoRaytraceFile)).readlines():
                if line.startswith('outerscale %s' %(screen)):
                    name, num, out = line.split()
            print 'Outerscale: ', out
            low = float(out)*100.0
            print 'Low', low
            if self.filt == '0':
                wav = 0.36
            elif self.filt == '1':
                wav = 0.48
            elif self.filt == '2':
                wav = 0.62
            elif self.filt == '3':
                wav = 0.76
            elif self.filt == '4':
                wav = 0.87
            else:
                wav = 0.97
            atmoScreen = 'atmospherescreen_%s_%s' %(self.obshistid, screen)
            cmd = 'time ./turb2d -seed %s%s -see5 %s -outerx 50000.0 -outers %s -zenith %s -wavelength %s -name %s' %(self.obsid, screen, self.rawseeing, low, self.zen, wav, atmoScreen)
            sys.stderr.write('Running: %s\n'% cmd)
            with WithTimer() as t:
              subprocess.check_call(cmd, shell=True)
            t.PrintWall('turb2d', sys.stderr)

            shutil.move('%s_density_coarse.fits' %(atmoScreen), '../../')
            shutil.move('%s_density_medium.fits' %(atmoScreen), '../../')
            shutil.move('%s_density_fine.fits' %(atmoScreen), '../../')
            shutil.move('%s_coarsex.fits' %(atmoScreen), '../../')
            shutil.move('%s_coarsey.fits' %(atmoScreen), '../../')
            shutil.move('%s_finex.fits' %(atmoScreen), '../../')
            shutil.move('%s_finey.fits' %(atmoScreen), '../../')
            shutil.move('%s_mediumx.fits' %(atmoScreen), '../../')
            shutil.move('%s_mediumy.fits' %(atmoScreen), '../../')
            with file('../../%s' %(self.atmoRaytraceFile), 'a') as parFile:
                parFile.write('atmospherefile %s ../%s \n' %(screen, atmoScreen))

        os.chdir('../../')

        return wav

    def generateCloudScreen(self):

        """
        (4) Create the cloud screens.
        """
        print 'Generating Cloud Screens.'

        try:
            os.remove(self.cloudRaytraceFile)
        except:
            #print 'WARNING: No file %s to remove!' %(self.cloudRaytraceFile)
            pass

        os.chdir('ancillary/atmosphere')

        screenNumber = [0, 3]
        for screen in screenNumber:
            for line in open('../../%s' %(self.atmoRaytraceFile)).readlines():
                if line.startswith('height %s' %(screen)):
                    name, num, height = line.split()
            print 'Height: ', height
            cloudScreen = 'cloudscreen_%s_%s' %(self.obshistid, screen)
            cmd = 'time ./cloud -seed %s%s -height %s -name %s -pix 100' %(self.obsid, screen, height, cloudScreen)
            sys.stderr.write('Running: %s\n'% cmd)
            with WithTimer() as t:
              subprocess.check_call(cmd, shell=True)
            t.PrintWall('cloud', sys.stderr)
            shutil.move('%s.fits' %(cloudScreen), '../../')
            with file('../../%s' %(self.cloudRaytraceFile), 'a') as parFile:
                parFile.write('cloudfile %s ../%s \n' %(screen, cloudScreen))
        os.chdir('../..')

        return

    def generateControlParams(self):

        """
        (5) Create the control parameter files.
        """

        print 'Creating Control and Optics Parameter Files.'

        if os.path.isfile(self.controlParFile):
            os.remove(self.controlParFile)
        with file(self.controlParFile, 'a') as parFile:
            parFile.write('outputfilename %s \n' %(self.opticsParFile))
            #parFile.write('detectormode 0 \n')
            parFile.write('zenith %s \n' %(self.zen))
            parFile.write('ranseed %s \n' %(self.obsid))
            parFile.write('optics_parameters \n')

        os.chdir('ancillary/optics_parameters')
        cmd = 'time ./optics_parameters < ../../%s' %(self.controlParFile)
        sys.stderr.write('Running: %s\n'% cmd)
        with WithTimer() as t:
          subprocess.check_call(cmd, shell=True)
        t.PrintWall('optics_parameters', sys.stderr)
        shutil.move('%s' %(self.opticsParFile), '../../')
        os.chdir('../../')

        return


    def generateTrackingParams(self):

        """

        (6) New for PT1.2 - added tracking generator parameter file
            generation.

        """
        print 'Creating Tracking Parameter Files.'
        trackParFile = self.trackParFile

        try:
            os.remove(self.trackParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(trackParFile)
            pass

        with file(self.trackParFile, 'a') as parFile:
            parFile.write('outputfilename %s \n' %(self.trackingParFile))
            parFile.write('seed %s \n' %(self.obsid))
            parFile.write('starttime %s \n' %(self.starttime))
            parFile.write('endtime %s \n' %(self.endtime))
            parFile.write('tracking \n')

        os.chdir('ancillary/tracking')
        cmd = 'time ./tracking < ../../%s' %(self.trackParFile)
        sys.stderr.write('Running: %s\n'% cmd)
        with WithTimer() as t:
          subprocess.check_call(cmd, shell=True)
        t.PrintWall('tracking', sys.stderr)
        shutil.move('%s' %(self.trackingParFile), '../../')
        os.chdir('../../')

        return

    def writeSedManifest(self, trimCatFile, cid):
        """
        Use the output of the 'trim' program, specifically trimcatalog_*.pars, to
        figure out which SEDs are needed from the shared catalog for chip 'cid'.
        Generate a manifest of these and write it to sedlist_*.txt.

        For the moment, just do this via a shell command rather than loading
        everything into Python.
        """
        cmd = 'cat %s ' % trimCatFile
        #cmd = cmd + '| egrep \'starSED|galaxySED|ssmSED|agnSED|flatSED|sky\' | awk \'{print "../data/"$6", \\\\"}\' '
        cmd = cmd + '| egrep \'starSED|galaxySED|ssmSED|agnSED|flatSED|sky\' | awk \'{print $6 }\' '
        cmd = cmd + '| sort | uniq > %s' % ParsFilenames(self.obshistid).sedlist(cid)
        print 'Executing command:'
        print '  ' + cmd
        subprocess.check_call(cmd, shell=True)



    def generateTrimCatalog(self):
        """
        Run trim program to create trimcatalog_*.pars files for each chip.
        """
        raftid = ""
        # Progress through the list of cids.  For the first cid of every raft,
        # create a trim par file.  For the last cid of every raft, run trim.
        for i,elt in enumerate(self.cidList):
            cid = elt[0]
            if raftid != cid.split("_")[0]:
                raftid = cid.split("_")[0]
                print 'Submitting raft', raftid
                trimParFile = 'trim_%s_%s.pars' %(self.obshistid, raftid)
                if os.path.isfile(trimParFile):
                    os.remove(trimParFile)
                with file(trimParFile, 'a') as parFile:
                    parFile.write('ncatalog %s \n' %(self.ncat))
                # Add the catalogs to the trim parameter file for the trim program
                cmd = 'cat %s >> %s' %(self.catListFile, trimParFile)
                subprocess.check_call(cmd, shell=True)
                chipcounter = 0
            print 'Submitting chip:', cid
            trimCatFile = 'trimcatalog_%s_%s.pars' %(self.obshistid, cid)
            with file(trimParFile, 'a') as parFile:
                parFile.write('out_file %s %s \n' %(chipcounter, trimCatFile))
                parFile.write('chip_id %s %s \n' %(chipcounter, cid))
            chipcounter += 1
            # If the next chip is in a different raft (or this is the last chip),
            # run trim program
            if i == len(self.cidList) - 1:
                print 'Last chipid =', cid
                nextRaftid = ""
            else:
                nextRaftid = self.cidList[i+1][0].split("_")[0]
            if nextRaftid != raftid:
                # If running a single sensor, ntrims needs to be 1 or it won't run
                if self.idonly:
                    ntrims = 1
                else:
                    ntrims = chipcounter
                    #TODO This is an LSST-specific test.  Remove later.
                    assert chipcounter == 9
                with file(trimParFile, 'a') as parFile:
                    parFile.write('ntrim %s \n' %(ntrims))
                    parFile.write('point_ra %s \n' %(self.pra))
                    parFile.write('point_dec %s \n' %(self.pdec))
                    parFile.write('rot_ang %s \n' %(self.prot))
                    parFile.write('buffer 100 \n')
                    parFile.write('straylight 0 \n')
                    #TODO: parFile.write('flatdir 1 \n')
                    parFile.write('trim \n')

                print 'Running TRIM for cid %s.' %cid
                os.chdir('ancillary/trim')
                cmd = 'time ./trim < ../../%s' %(trimParFile)
                sys.stderr.write('Running: %s\n'% cmd)
                with WithTimer() as t:
                  subprocess.check_call(cmd, shell=True)
                t.PrintWall('trim', sys.stderr)
                print 'Finished Running TRIM.'
                os.chdir('../..')
                os.remove(trimParFile)
                
        # Now move the trimcatalog files
        for elt in self.cidList:
            cid = elt[0]
            trimCatFile = os.path.join('ancillary/trim',
                                       'trimcatalog_%s_%s.pars' %(self.obshistid, cid))
            with file(trimCatFile, 'a') as parFile:
                parFile.write('lsst \n')
            shutil.move(trimCatFile, '.')
            print 'Processed trimcatalog file %s.' %(trimCatFile)
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
            print 'minsource', self.minsource
            if nTrimCatSources >= self.minsource:

                # Figure out which SED files are needed for this chip and store
                # in sedlist_*.txt.  This information is useful for platforms
                # where we stage only the needed SED files.
                self.writeSedManifest(trimCatFile, cid)
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
                    nexp = self.nsnap
                    exptime = (self.vistime - (nexp-1) * devvalue) / nexp
                elif devtype == 'CMOS':
                    nexp = int(self.vistime / devvalue)
                    exptime = self.vistime / nexp
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
                        timeoff = 0.5*exptime + ex*(devvalue + exptime) - 0.5*self.vistime
                    elif devtype == 'CMOS':
                        timeoff = 0.5*exptime + ex*exptime - 0.5*self.vistime
                    else:
                        raise RuntimeError, "Unknown devtype=%s in focalplanelayout file:" %devtype
                    with file(timeParFile, 'a') as parFile:
                        parFile.write('timeoffset %f \n' %timeoff)
                        parFile.write('pairid %d \n' %ex)
                        parFile.write('exptime %f \n' %exptime)
                    seedchip = int(self.obsid) + cc*1000 + ex
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
                    self.generateRaytraceParams(id, chipParFile, seedchip, timeParFile,
                                           raytraceParFile)
                    # GENERATE THE BACKGROUND ADDER PARS
                    print 'Generating background pars.'
                    backgroundParFile = parNames.background(id)
                    self.generateBackgroundParams(id, seedchip, cid, wav, backgroundParFile)
                    # GENERATE THE COSMIC RAY ADDER PARS
                    print 'Generating cosmic rays pars'
                    cosmicParFile = parNames.cosmic(id)
                    self.generateCosmicRayParams(id, seedchip, exptime, cosmicParFile)
                    # GENERATE THE ELECTRON TO ADC CONVERTER PARS
                    print 'Generating e2adc pars.'
                    self.generateE2adcParams(id, cid, expid, seedchip, exptime)

                    if self.idonly:
                        try:
                            os.mkdir(self.scratchOutputDir)
                        except:
                            print 'WARNING: Directory %s already exists!' %(self.scratchOutputDir)
                            pass
                        makeChipImage(self.obshistid, self.filt, cid, expid, self.scratchOutputPath)
                    else:
                        # MAKE THE SINGLE-CHIP SCRIPTS
                        print 'Making Single-Chip Scripts.'
                        scriptGen.makeScript(cid, expid, raytraceParFile, backgroundParFile,
                                             cosmicParFile, expTrimCatFile, self.logPath)
                    print 'Count:', count
                    count += 1
                    ex += 1
            os.remove(trimCatFile)  # if nTrimCatSources >= self.minsource:
        return



    def generateRaytraceParams(self, id, chipParFile, seedchip, timeParFile, raytraceParFile):

        """
        (8) Create and return the LSST (Raytrace) parameter file.
        """

        with file(chipParFile, 'a') as parFile:
            parFile.write('outputfilename imsim_%s_%s \n' %(self.obshistid, id))
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('trackingfile ../%s \n' %(self.trackingParFile))

        # Adds contents of extra table to chipParFile.  Extra table
        # parameters are used to turn parameters on and off (eg. clouds) in
        # the simulator. Clouds are 'on' by default.
        if self.extraidFile != '':
            cmd = 'cat %s >> %s' %(self.extraidFile, chipParFile)
            subprocess.check_call(cmd, shell=True)

        cmd = 'cat %s %s %s %s %s %s > %s' %(self.obsParFile, self.atmoRaytraceFile, self.opticsParFile, timeParFile, self.cloudRaytraceFile, chipParFile, raytraceParFile)
        #JPG: Added the 'straylight' line because it's in the v-3.0 condor file:
        with file(raytraceParFile, 'a') as parFile:
          parFile.write('straylight 0 \n')
        subprocess.check_call(cmd, shell=True)

        return

    def generateBackgroundParams(self, id, seedchip, cid, wav, backgroundParFile):

        """
        (9) Create and return the BACKGROUND parameter file.
        """

        try:
            os.remove(backgroundParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(backgroundParFile)
            pass

        for line in open(self.atmoRaytraceFile).readlines():
            if line.startswith('relh2o'):
                name, watervar = line.split()

        with file(backgroundParFile, 'a') as parFile:
            parFile.write('out_file imsim_%s_%s.fits \n' %(self.obshistid, id))
            parFile.write('point_alt %s \n' %(self.alt))
            parFile.write('point_az %s \n' %(self.az))
            parFile.write('filter %s \n' %(self.filt))
            parFile.write('spiderangle %s \n' %(self.spid))
            parFile.write('rot_ang %s \n' %(self.prot))
            parFile.write('chip_id %s \n' %(cid))
            parFile.write('solar_zen %s \n' %(self.sunzen))
            if self.telconfig == '0':
                parFile.write('zenith_v 22.09 \n')
                parFile.write('watervar %s \n' %(watervar))
            elif self.telconfig == '1':
                parFile.write('zenith_v 10000.0 \n')
            elif self.telconfig == '2':
                parFile.write('zenith_v 18.00 \n')
                parFile.write('fc_file filter_constants_dome \n')
            else:
                parFile.write('zenith_v 21.00 \n')
                parFile.write('fc_file filter_constants_dome \n')
            parFile.write('moon_alt %s \n' %(self.moonalt))
            parFile.write('moon_ra %s \n' %(self.mra))
            parFile.write('moon_dec %s \n' %(self.mdec))
            parFile.write('point_ra %s \n' %(self.pra))
            parFile.write('point_dec %s \n' %(self.pdec))
            parFile.write('moon_dist %s \n' %(self.moondist))
            parFile.write('phase_ang %s \n' %(self.moonphaserad))
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('wavelength %s \n' %(wav))
            cmd = 'grep cloudmean %s >> %s' %(self.atmoRaytraceFile, backgroundParFile)
            subprocess.check_call(cmd, shell=True)
            parFile.write('add_background \n')

        return

    def generateCosmicRayParams(self, id, seedchip, exptime, cosmicParFile):

        """
        (10) Create and return the COSMIC_RAY parameter file.
        """

        try:
            os.remove(cosmicParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(cosmicParFile)
            pass

        with file(cosmicParFile, 'a') as parFile:
            parFile.write('inputfilename ../Add_Background/fits_files/imsim_%s_%s.fits \n' %(self.obshistid, id))
            parFile.write('outputfilename output_%s_%s.fits \n' %(self.obshistid, id))
            parFile.write('pixsize 10.0 \n')
            parFile.write('exposuretime %s \n' %(exptime))
            parFile.write('raydensity 0.6 \n')
            parFile.write('scalenumber 8.0 \n')
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('createrays \n') 

        return

    def generateE2adcParams(self, id, cid, expid, seedchip, exptime):

        """
        (11) Create and return the E2ADC parameter file.
        """

        e2adcParFile = ParsFilenames(self.obshistid).e2adc(id)
        cmd = 'cat data/focal_plane/sta_misalignments/readout/readoutpars_%s >> %s' \
              %(cid, e2adcParFile)
        subprocess.check_call(cmd, shell=True)
        with file(e2adcParFile, 'a') as parFile:
            parFile.write('inputfilename ../cosmic_rays/output_%s_%s.fits \n' %(self.obshistid, id))
            parFile.write('outputprefilename imsim_%s_ \n' % self.obshistid )
            parFile.write('outputpostfilename _%s \n' % expid) 
            parFile.write('chipid %s \n' % cid)
            parFile.write('qemapfilename ../../data/focal_plane/sta_misalignments/qe_maps/QE_%s.fits.gz \n' % cid)
            parFile.write('exptime %s \n'%(exptime))
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('e2adc \n')
        return

    def _stageAndCleanupFiles(self):
        """

        Make tarball of needed files for local nodes, create directories
        on shared file system for logs and images (if not copying to IRODS
        storage on Pogo1) and cleanup home directory by moving all files
        to 'run' directory.

        """

        if not self.idonly:
            # Create tar file to be copied to local nodes.  This prevents
            # IO issues with copying lots of little files from the head
            # node to the local nodes.  Only create the tar file if not
            # running in single chip mode.

            print 'Tarring all redundant files that will be copied to the local cluster nodes.'
            nodeFilesTar = 'nodeFiles%s.tar' % self.obshistid

            os.chdir('%s' %(self.workDir))
            # Files needed for ancillary/atmosphere
            print 'Tarring atmosphere files.'
            cmd =  'tar cf %s' % nodeFilesTar
            cmd += ' atmospherescreen_%s_*.fits cloudscreen_%s_*.fits' %(self.obshistid, self.obshistid)
            cmd += ' %s %s %s' %(self.cloudRaytraceFile, self.controlParFile, self.obsCatFile)
            subprocess.check_call(cmd, shell=True)

            # LSST parameter files
            print 'Tarring lsst.'
            cmd =  'tar rf %s lsst/*.txt' % nodeFilesTar
            subprocess.check_call(cmd, shell=True)
            
            # Files for ancillary/Add_Background
            #print 'Tarring ancillary/Add_Background/ files.'
            #shutil.move(nodeFilesTar, 'ancillary/Add_Background')
            #os.chdir('ancillary/Add_Background')
            #shutil.copy('SEDs/darksky_sed.txt', '.')
            #shutil.copy('SEDs/lunar_sed.txt', '.')
            #shutil.copy('SEDs/sed_dome.txt', '.')
            #cmd = 'tar rvf nodeFiles%s.tar filter_constants darksky_sed.txt lunar_sed.txt sed_dome.txt filter_constants_dome' %(self.obshistid)
            #subprocess.check_call(cmd, shell=True)
            #os.remove('darksky_sed.txt')
            #os.remove('lunar_sed.txt')
            #os.remove('sed_dome.txt')
            #shutil.move(nodeFilesTar, '../../')
            #os.chdir('../../')
            
            # Executables and binaries files for running on nodes.
            self._tarExecFiles(nodeFilesTar)

            # Zip the tar file.
            print 'Gzipping %s' % nodeFilesTar
            cmd = 'gzip %s' % nodeFilesTar
            subprocess.check_call(cmd, shell=True)
            nodeFilesTar += '.gz'
            # Move to stagePath2
            self._stageNodeFilesTarball(nodeFilesTar)

        # Move the parameter, and fits files to the run directory.
        self._cleanupFitsFiles()
        self._cleanupParFiles()
        self._cleanupSedFiles()

        # Move the script files if created (i.e. not in single-chip mode)
        if not self.idonly:
            self._cleanupScriptFiles()
        return

    def _tarExecFiles(self, nodeFilesTar):
        print 'Tarring binaries/executables.'
        cmd = 'tar rf %s ancillary/trim/trim ancillary/Add_Background/* ancillary/cosmic_rays/* ancillary/e2adc/e2adc raytrace/lsst raytrace/*.txt  raytrace/version pbs/distributeFiles.py Exposure.py Focalplane.py verifyFiles.py chip.py' % nodeFilesTar
        subprocess.check_call(cmd, shell=True)
        return

    def _stageNodeFilesTarball(self, nodeFilesTar):
        print 'Moving %s to %s/.' %(nodeFilesTar, self.stagePath2)
        if os.path.isfile(os.path.join(self.stagePath2, nodeFilesTar)):
            try:
                os.remove(os.path.join(self.stagePath2, nodeFilesTar))
            except OSError:
                pass
        shutil.move('nodeFiles%s.tar.gz' %(self.obshistid), '%s/' %(self.stagePath2))

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
        scriptListFilename = generateRaytraceJobManifestFilename(self.obshistid, self.filter)
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
        #try:
        #    cmd = 'ls %s/*.csh > %s' %(self.paramDir, scriptListFilename)
        #    subprocess.check_call(cmd,shell=True)
        #    print 'Created qsub list.'
        #    print 'Finished tarring and moving files.  Ready to launch per-chip scripts.'
        #except:
        #    print 'WARNING: No shell script files to list!'
        #    pass
        return



class AllChipsScriptGenerator_Pbs(AllChipsScriptGenerator):

    def __init__(self, trimfile, policy, extraidFile):
        AllChipsScriptGenerator.__init__(self, trimfile, policy, extraidFile)
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
        scriptGen = SingleChipScriptGenerator_Pbs(self.policy, self.obshistid, self.filter,
                                                  self.filt, self.centid, self.centroidPath,
                                                  self.stagePath2, self.paramDir,
                                                  self.trackingParFile)
        self._loopOverChips(scriptGen, wav)
        return

    def _cleanupScriptFiles(self):
        scriptListFilename = generateRaytraceJobManifestFilename(self.obshistid, self.filter)
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

        #try:
        #    cmd = 'ls %s/*.pbs > %s' %(self.paramDir, scriptListFilename)
        #    subprocess.check_call(cmd,shell=True)
        #    print 'Created qsub list.'
        #    print 'Finished tarring and moving files.  Ready to launch PBS scripts.'
        #except:
        #    raise RuntimeError,  'WARNING: No PBS files to list!'
