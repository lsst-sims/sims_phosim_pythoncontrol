#!/usr/bin/python

"""
Brief:   This class sets generates all of the scripts needed for the ray-tracing
         phase and beyond, i.e. all of the per-chip scripts.

Date:    January 26, 2012
Authors: Nicole Silvestri, U. Washington, nms21@uw.edu,
         Jeff Gardner, U. Washington, Google, gardnerj@phys.washington.edu
"""

from __future__ import with_statement
import os, re, sys
import datetime
import glob
import math
import shutil
import subprocess
import string
import time
import chip

from SingleChipScriptGenerator import *
#import lsst.pex.policy as pexPolicy
#import lsst.pex.logging as pexLog
#import lsst.pex.exceptions as pexExcept

class ParFileNameFactory:
    """
    A simple class to try to keep all the filename definitions in one place.
    key:
      ex  = 0 or 1
      id  = 'R'+rx+ry+'_'+'S'+sx+sy+'_'+'E00'+ex

    """
    def time(self, obshistid, ex):
        return 'time_%s_E00%s.pars' %(obshistid, ex)

    def chip(self, obshistid, id):
        return 'chip_%s_%s.pars' %(obshistid, id)

    def raytrace(self, obshistid, id):
        return 'raytracecommands_%s_%s.pars' %(obshistid, id)

    def background(self, obshistid, id):
        return 'background_%s_%s.pars' %(obshistid, id)

    def cosmic(self, obshistid, id):
        return 'cosmic_%s_%s.pars' %(obshistid, id)


class AllChipsScriptGenerator:
    """
    This class sets generates all of the scripts needed for the ray-tracing
    phase and beyond, i.e. all of the per-chip scripts.  It calls
    SingleChipScriptGenerator.makeScript() twice for each chip (2 exposures
    per chip) in loopOverChips().

    This is the class that is least changed from Nicole's original version,
    mostly because it has so few scheduler dependencies.  Like Nicole's version,
    you can call the class with rx,ry,sx,sy,ex and it will only run a single
    chip instead of the full focal plane.
    """

    def __init__(self, trimfile, policy, extraidFile, rx, ry, sx, sy, ex):

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

        #
        # LSST-specific params
        #

        self._readTrimfileAndCalculateParams(rx, ry, sx, sy, ex)

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

        # Parameter File Names
        self.obsCatFile = 'objectcatalog_%s.pars' %(self.obshistid)
        self.obsParFile = 'obs_%s.pars' %(self.obshistid)
        self.atmoParFile = 'atmosphere_%s.pars' %(self.obshistid)
        self.atmoRaytraceFile = 'atmosphereraytrace_%s.pars' %(self.obshistid)
        self.cloudRaytraceFile = 'cloudraytrace_%s.pars' %(self.obshistid)
        self.opticsParFile = 'optics_%s.pars' %(self.obshistid)
        self.catListFile = 'catlist_%s.pars' %(self.obshistid)
        self.trackingParFile = 'tracking_%s.pars' %(self.obshistid)

        return

    def _readTrimfileAndCalculateParams(self, rx, ry, sx, sy, ex):
        self.myrx = rx
        self.myry = ry
        self.mysx = sx
        self.mysy = sy
        self.myex = ex

        print 'Using instance catalog: ', self.trimfile
        print '***'

        print 'Initializing Opsim and Instance Catalog Parameters.'
        for line in open(self.trimfile).readlines():
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
            #if line.startswith('SIM_CAMCONFIG'):
            #    name, self.camconfig = line.split()
            #    print 'Sim_Camconfig: ', self.camconfig
            if line.startswith('SIM_VISTIME'):
                name, self.vistime = line.split()
                print 'Sim_Vistime: ', self.vistime

        # Calculated Parameters
        tempDate = datetime.date.today()
        sDate = str(tempDate)
        year, mo, day = sDate.split('-')

        self.readtime = 3.0
        print 'Readtime:', self.readtime
        self.exptime = 0.5*(float(self.vistime)) - 0.5*(float(self.readtime))
        print 'Exptime:', self.exptime
        self.timeoff = 0.5*(float(self.exptime)) + 0.5*(float(self.readtime))
        print 'Timeoff:', self.timeoff
        self.starttime = -0.5*(float(self.vistime))
        print 'StartTime:', self.starttime
        self.endtime = 0.5*(float(self.vistime))
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
	self.minsource += 1
        self.ncat = 0
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


    def makeScripts(self):
        """
        This is the main worker routine.  It just goes through and calls
        all of Nicole's original functions.
        """
        self._makePaths()
        self.writeObsCatParams()
        self.generateAtmosphericParams()
        wav = self.generateAtmosphericScreen()
        self.generateCloudScreen()
        self.generateControlParams()
        self.generateTrackingParams()
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator(self.policy, self.obshistid, self.filter,
                                              self.filt, self.centid, self.centroidPath,
                                              self.stagePath2, self.paramDir,
                                              self.trackingParFile)
        self.loopOverChips(scriptGen, wav)
        self.cleanup()


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
            parFile.write('exptime %s \n' %(self.exptime))

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
        cmd = './create_atmosphere < ../../%s' %(self.atmoParFile)
        subprocess.check_call(cmd, shell=True)
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
            cmd = './turb2d -seed %s%s -see5 %s -outerx 50000.0 -outers %s -zenith %s -wavelength %s -name %s' %(self.obsid, screen, self.rawseeing, low, self.zen, wav, atmoScreen)
            print cmd
            subprocess.check_call(cmd, shell=True)

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
            cmd = './cloud -seed %s%s -height %s -name %s -pix 100' %(self.obsid, screen, height, cloudScreen)
            subprocess.check_call(cmd, shell=True)
            print cmd
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
        controlParFile = 'control_%s.pars' %(self.obshistid)

        try:
            os.remove(self.controlParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(controlParFile)
            pass

        with file(controlParFile, 'a') as parFile:
            parFile.write('outputfilename %s \n' %(self.opticsParFile))
            #parFile.write('detectormode 0 \n')
            parFile.write('zenith %s \n' %(self.zen))
            parFile.write('ranseed %s \n' %(self.obsid))
            parFile.write('optics_parameters \n')

        os.chdir('ancillary/optics_parameters')
        cmd = './optics_parameters < ../../%s' %(controlParFile)
        subprocess.check_call(cmd, shell=True)
        shutil.move('%s' %(self.opticsParFile), '../../')
        os.chdir('../../')

        return


    def generateTrackingParams(self):

        """

        (6) New for PT1.2 - added tracking generator parameter file
            generation.

        """
        print 'Creating Tracking Parameter Files.'
        trackParFile = 'track_%s.pars' %(self.obshistid)

        try:
            os.remove(trackParFile)
        except:
            #print 'WARNING: No file %s to remove!' %(trackParFile)
            pass

        with file(trackParFile, 'a') as parFile:
            parFile.write('outputfilename %s \n' %(self.trackingParFile))
            parFile.write('seed %s \n' %(self.obsid))
            parFile.write('starttime %s \n' %(self.starttime))
            parFile.write('endtime %s \n' %(self.endtime))
            parFile.write('tracking \n')

        os.chdir('ancillary/tracking')
        cmd = './tracking < ../../%s' %(trackParFile)
        subprocess.check_call(cmd, shell=True)
        shutil.move('%s' %(self.trackingParFile), '../../')
        os.chdir('../../')

        return

    def writeSedManifest(self, trimCatFile, cid):
        """
        Use the output of the "trim" program, specifically trimcatalog_*.pars, to
        figure out which SEDs are needed from the shared catalog for chip 'cid'.
        Generate a manifest of these and write it to sedlist_*.txt.

        For the moment, just do this via a shell command rather than loading
        everything into Python.
        """
        cmd = 'cat %s ' % trimCatFile
        #cmd = cmd + '| egrep \'starSED|galaxySED|ssmSED|agnSED|flatSED|sky\' | awk \'{print "../data/"$6", \\\\"}\' '
        cmd = cmd + '| egrep \'starSED|galaxySED|ssmSED|agnSED|flatSED|sky\' | awk \'{print $6 }\' '
        cmd = cmd + '| sort | uniq > sedlist_%s_%s.txt' %(self.obshistid, cid)
        print 'Executing command:'
        print '  ' + cmd
        subprocess.check_call(cmd, shell=True)



    def loopOverChips(self, scriptGen, wav):

        """

        (7) Trim Program: Create the parameter files for each chip for
        each stage of the Simulator code then call the python code to
        create all of the scripts for visit (378 script files will be
        generated).

        """
        parFactory = ParFileNameFactory()
        count = 1
        rxList = ['0', '1', '2', '3', '4']
        ryList = ['0', '1', '2', '3', '4']
        if self.myrx != '':
            rxList = ['%s' %(self.myrx)]
            ryList = ['%s' %(self.myry)]
        for rx in rxList:
            for ry in ryList:
                print 'Rx: ', rx
                print 'Ry: ', ry
                if rx+ry == '00':
                    print 'Skipping 00.'
                elif rx+ry == '04':
                    print 'Skipping 04.'
                elif rx+ry == '40':
                    print 'Skipping 40.'
                elif rx+ry == '44':
                    print 'Skipping 44.'
                else:
                    rid = 'R'+rx+ry
                    print 'Submitting raft', rid
                    trimParFile = 'trim_%s_%s.pars' %(self.obshistid, rid)

                    try:
                        os.remove(trimParFile)
                    except:
                        #print 'WARNING: No file %s to remove!' %(trimParFile)
                        pass

                    with file(trimParFile, 'a') as parFile:
                        parFile.write('ncatalog %s \n' %(self.ncat))

                    # Add the catalogs to the trim parameter file for the trim program
                    cmd = 'cat %s >> %s' %(self.catListFile, trimParFile)
                    subprocess.check_call(cmd, shell=True)
                    chipcounter = 0
                    sxList = ['0', '1', '2']
                    syList = ['0', '1', '2']
                    if self.mysx != '':
                        sxList = ['%s' %(self.mysx)]
                        syList = ['%s' %(self.mysy)]
                    for sx in sxList:
                        for sy in syList:
                            cid = 'R'+rx+ry+'_'+'S'+sx+sy
                            print 'Submitting chip:', cid
                            trimCatFile = 'trimcatalog_%s_%s.pars' %(self.obshistid, cid)
                            with file(trimParFile, 'a') as parFile:
                                parFile.write('out_file %s %s \n' %(chipcounter, trimCatFile))
                                parFile.write('chip_id %s %s \n' %(chipcounter, cid))
                            chipcounter += 1

                    # If running a single sensor, ntrims needs to be 1 or it won't run
                    if self.mysx != '':
                        ntrims = 1
                    else:
                        ntrims = 9

                    with file(trimParFile, 'a') as parFile:
                        parFile.write('ntrim %s \n' %(ntrims))
                        parFile.write('point_ra %s \n' %(self.pra))
                        parFile.write('point_dec %s \n' %(self.pdec))
                        parFile.write('rot_ang %s \n' %(self.prot))
                        parFile.write('buffer 100 \n')
                        parFile.write('straylight 0 \n')
                        #TODO: parFile.write('flatdir 1 \n')
                        parFile.write('trim \n')

                    print 'Running TRIM.'
                    os.chdir('ancillary/trim')
                    cmd = './trim < ../../%s' %(trimParFile)
                    subprocess.check_call(cmd, shell=True)
                    print 'Finished Running TRIM.'

                    # Postprocess the trimcatalog*.pars files
                    sxList = ['0', '1', '2']
                    syList = ['0', '1', '2']
                    if self.mysx != '':
                        sxList = ['%s' %(self.mysx)]
                        syList = ['%s' %(self.mysy)]
                    for sx in sxList:
                        for sy in syList:
                            cid = 'R'+rx+ry+'_'+'S'+sx+sy
                            print 'Working on chip:', cid
                            trimCatFile = 'trimcatalog_%s_%s.pars' %(self.obshistid, cid)
                            with file(trimCatFile, 'a') as parFile:
                                parFile.write('lsst \n')

                            shutil.move('%s' %(trimCatFile), '../..')
                            print 'Finished writing trimcatalog file %s.' %(trimCatFile)

                    os.chdir('../..')

                    sxList = ['0', '1', '2']
                    syList = ['0', '1', '2']
                    if self.mysx != '':
                        sxList = ['%s' %(self.mysx)]
                        syList = ['%s' %(self.mysy)]
                    for sx in sxList:
                        for sy in syList:
                            cid = 'R'+rx+ry+'_'+'S'+sx+sy
                            trimCatFile = 'trimcatalog_%s_%s.pars' %(self.obshistid, cid)
                            numLines = len(open(trimCatFile).readlines())
                            print 'numlines:', numLines
                            print 'minsource', self.minsource
                            # MINSOURCE=0 in trimfile header will produce background-only images.
                            # if numLines > 0: # creates script for the sensor designated by cid
                            #TEMP: This should be changed to numLines-2 > self.minsource since
                            #      the addition of "lsst" at the end means an empty file is 2 lines long.
                            if numLines > self.minsource:

                                # If useSharedSEDs is set to 'true', then we are going to read only
                                # the SEDs that we need directly from the shared storage location.
                                # So figure out which SED files are needed for this chip and store
                                # in sedlist_*.txt
                                if self.useSharedSEDs == True:
                                  self.writeSedManifest(trimCatFile, cid)

                                newTrimCatFile01 = 'trimcatalog_%s_%s_E001.pars' %(self.obshistid, cid)
                                newTrimCatFile00 = 'trimcatalog_%s_%s_E000.pars' %(self.obshistid, cid)
                                shutil.copyfile(trimCatFile, newTrimCatFile01)
                                shutil.copyfile(trimCatFile, newTrimCatFile00)

                                # LOOP OVER EXPOSURES
                                print 'Looping over Exposures'
                                exList = ['0', '1']
                                if self.myex != '':
                                    exList = ['%s' %(self.myex)]
                                for ex in exList:
                                    #timeParFile = 'time_%s_E00%s.pars' %(self.obshistid, ex)
                                    timeParFile = parFactory.time(self.obshistid, ex)
                                    try:
                                        os.remove(timeParFile)
                                    except:
                                        #print 'WARNING: No file %s to remove!' %(timeParFile)
                                        pass

                                    if ex=='0':
                                        with file(timeParFile, 'a') as parFile:
                                            parFile.write('timeoffset -%s \n' %(self.timeoff))
                                    else:
                                        with file(timeParFile, 'a') as parFile:
                                            parFile.write('timeoffset %s \n' %(self.timeoff))

                                    with file(timeParFile, 'a') as parFile:
                                        parFile.write('pairid %s \n' %(ex))

                                    id  = 'R'+rx+ry+'_'+'S'+sx+sy+'_'+'E00'+ex

                                    nrx = int(rx) * 90
                                    nry = int(ry) * 18
                                    nsx = int(sx) * 6
                                    nsy = int(sy) * 2
                                    seedchip = int(self.obsid) + nrx + nry + nsx + nsy + int(ex)

                                    #chipParFile = 'chip_%s_%s.pars' %(self.obshistid, id)
                                    chipParFile = parFactory.chip(self.obshistid, id)
                                    try:
                                        os.remove(chipParFile)
                                    except:
                                        #print 'WARNING: No file %s to remove!' %(chipParFile)
                                        pass

                                    shutil.copyfile('data/focal_plane/sta_misalignments/offsets/pars_%s' %(cid), chipParFile)
                                    with file(chipParFile, 'a') as parFile:
                                        #TODO: parFile.write('flatdir 1 \n')
                                        parFile.write('chipid %s \n' %(cid))
                                        parFile.write('chipheightfile ../data/focal_plane/sta_misalignments/height_maps/%s.fits.gz \n' %(cid))

                                    raytraceParFile   = parFactory.raytrace(self.obshistid, id)
                                    backgroundParFile = parFactory.background(self.obshistid, id)
                                    cosmicParFile     = parFactory.cosmic(self.obshistid, id)
                                    # GENERATE THE RAYTRACE PARS
                                    print 'Running the raytrace.'
                                    self.generateRaytraceParams(id, chipParFile, seedchip, timeParFile,
                                                           raytraceParFile)
                                    # GENERATE THE BACKGROUND ADDER PARS
                                    print 'Running the background.'
                                    self.generateBackgroundParams(id, seedchip, cid, wav, backgroundParFile)
                                    # GENERATE THE COSMIC RAY ADDER PARS
                                    print 'Running cosmic rays.'
                                    self.generateCosmicRayParams(id, seedchip, cosmicParFile)
                                    # GENERATE THE ELECTRON TO ADC CONVERTER PARS
                                    print 'Running e2adc.'
                                    self.generateE2adcParams(id, cid, ex, rx, ry, sx, sy)
                                    #jId = self.obshistid + ex + str(count)
                                    #jobId = int(jId)

                                    sensorId  = rx+ry+'_'+sx+sy+'_'+ex
                                    if self.myrx !='':
                                        try:
                                            os.mkdir(self.scratchOutputDir)
                                        except:
                                            print 'WARNING: Directory %s already exists!' %(self.scratchOutputDir)
                                            pass
                                        chip.makeChipImage(self.obshistid, self.filt, rx, ry, sx, sy, ex, self.scratchOutputPath)
                                    else:
                                        # MAKE THE SINGLE-CHIP SCRIPTS
                                        print 'Making Single-Chip Scripts.'
                                        scriptGen.makeScript(cid, id, rx, ry, sx, sy, ex, raytraceParFile,
                                                             backgroundParFile, cosmicParFile, sensorId, self.logPath)
                                    print 'Count:', count
                                    count += 1
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

    def generateCosmicRayParams(self, id, seedchip, cosmicParFile):

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
            parFile.write('exposuretime %s \n' %(self.exptime))
            parFile.write('raydensity 0.6 \n')
            parFile.write('scalenumber 8.0 \n')
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('createrays \n') 

        return

    def generateE2adcParams(self, id, cid, ex, rx, ry, sx, sy):

        """
        (11) Create and return the E2ADC parameter file.
        """

        nrx = int(rx) * 90
        nry = int(ry) * 18
        nsx = int(sx) * 6
        nsy = int(sy) * 2
        seedchip = int(self.obsid) + nrx + nry + nsx + nsy + int(ex)
        e2adcParFile = 'e2adc_%s_%s.pars' %(self.obshistid, id)
        cmd = 'cat data/focal_plane/sta_misalignments/readout/readoutpars_%s >> %s' \
              %(cid, e2adcParFile)
        subprocess.check_call(cmd, shell=True)
        with file(e2adcParFile, 'a') as parFile:
            parFile.write('inputfilename ../cosmic_rays/output_%s_%s.fits.gz \n' %(self.obshistid, id))
            parFile.write('outputprefilename imsim_%s_ \n' % self.obshistid )
            parFile.write('outputpostfilename _E00%s \n' % ex) 
            parFile.write('chipid %s \n' % cid)
            parFile.write('qemapfilename ../../data/focal_plane/sta_misalignments/qe_maps/QE_%s.fits.gz \n' % cid)
            parFile.write('exptime %s \n'%(self.exptime))
            parFile.write('seed %s \n' %(seedchip))
            parFile.write('e2adc \n')
        return

    def cleanup(self):
        """

        Make tarball of needed files for local nodes, create directories
        on shared file system for logs and images (if not copying to IRODS
        storage on Pogo1) and cleanup home directory by moving all files
        to 'run' directory.

        """

        if self.myrx == '':
            # Create tar file to be copied to local nodes.  This prevents
            # IO issues with copying lots of little files from the head
            # node to the local nodes.  Only create the tar file if not
            # running in single chip mode.

            print 'Tarring all redundant files that will be copied to the local cluster nodes.'
            nodeFilesTar = 'nodeFiles%s.tar' % self.obshistid

            os.chdir('%s' %(self.workDir))
            # Files needed for ancillary/atmosphere
            print 'Tarring atmosphere files.'
            cmd =  'tar cvf %s' % nodeFilesTar
            cmd += ' atmospherescreen_%s_*.fits cloudscreen_%s_*.fits cloudraytrace_%s.pars' %(self.obshistid, self.obshistid, self.obshistid)
            cmd += ' control_%s.pars objectcatalog_%s.pars' %(self.obshistid, self.obshistid)
            subprocess.check_call(cmd, shell=True)

            # LSST parameter files
            print 'Tarring lsst files.'
            cmd =  'tar rvf %s lsst/*.txt' % nodeFilesTar
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
        if self.myrx == '':
            self._cleanupScriptFiles()
        return

    def _tarExecFiles(self, nodeFilesTar):
        print 'Tarring binaries/executables.'
        cmd = 'tar rvf %s ancillary/trim/trim ancillary/Add_Background/* ancillary/cosmic_rays/* ancillary/e2adc/e2adc raytrace/lsst raytrace/*.txt  raytrace/version pbs/distributeFiles.py chip.py' % nodeFilesTar
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
        print 'Moving .par files to %s.' %(self.paramDir)
        for pars in glob.glob('*.pars'):
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
        # Deal with the script and command files if created (not single chip mode).
        for pbs in glob.glob('exec_%s_*.csh' %(self.obshistid)):
            shutil.copy(pbs, '%s' %(self.paramDir))
            os.remove(pbs)

        try:
            for cmds in glob.glob('cmds_*.txt'):
                os.remove(cmds)
        except:
            print 'WARNING: No command files to remove!'
            pass


        try:
            cmd = 'ls %s/*.csh > %s_f%sJobs.lis' %(self.paramDir, self.obshistid, self.filter)
            subprocess.check_call(cmd,shell=True)
            print 'Created qsub list.'
            print 'Finished tarring and moving files.  Ready to launch per-chip scripts.'
        except:
            print 'WARNING: No shell script files to list!'
            pass
        return



class AllChipsScriptGenerator_Pbs(AllChipsScriptGenerator):

    def __init__(self, trimfile, policy, extraidFile, rx, ry, sx, sy, ex):
        AllChipsScriptGenerator.__init__(self, trimfile, policy, extraidFile, rx, ry, sx, sy, ex)
        self.username = self.policy.get('pbs','username')
        print 'Your PBS username is: ', self.username
        return

    def makeScripts(self):
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
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator_Pbs(self.policy, self.obshistid, self.filter,
                                                  self.filt, self.centid, self.centroidPath,
                                                  self.stagePath2, self.paramDir,
                                                  self.trackingParFile)
        self.loopOverChips(scriptGen, wav)
        self.cleanup()

    def _cleanupScriptFiles(self):
        # Deal with the pbs and command files if created (not single chip mode).
        for pbs in glob.glob('exec_%s_*.pbs' %(self.obshistid)):
            shutil.move(pbs, '%s' %(self.paramDir))

        try:
            for cmds in glob.glob('cmds_*.txt'):
                print 'Removing %s' %(cmds)
                os.remove(cmds)
        except:
            print 'WARNING: No command files to remove!'
            pass

        scriptListFilename = '%s_f%sJobs.lis' %(self.obshistid, self.filter)
        try:
            cmd = 'ls %s/*.pbs > %s' %(self.paramDir, scriptListFilename)
            subprocess.check_call(cmd,shell=True)
            print 'Created qsub list.'
            print 'Finished tarring and moving files.  Ready to launch PBS scripts.'
        except:
            print 'WARNING: No PBS files to list!'
            pass
