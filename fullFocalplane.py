#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python
############!/usr/bin/env python

"""
Brief:   Python script to create parameter files for each stage of the Image
         Simulator and to create PBS scripts for each sensor job per instance catalog.
         Adapted from Emily Grace & John Peterson's (Purdue U.) shell script for
         running on a Condor cluster at Purdue.
         Rewritten in python to work with PBS/MOAB/MAUI on the UW Cluster.

Date:    May 01, 2010
Authors: Nicole Silvestri, U. Washington, nms21@uw.edu,
         Jeff Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated: May 04, 2011 - in sync with revision 21185 of the simulator code trunk
         December 01, 2011 - Replaced pexPolicy usage with generic ConfigParser
                             so as to remove dependencies on LSST stack.

Usage:   python fullFocalplanePbs.py [options]
Options: trimfile:    absolute path and name of the trimfile
                      to process (unzipped)
         policy:      your copy of the imsimPbsPolicy.paf file
         extraidFile: name of the file containing extra parameters
                      to change simulator defaults (eg. turn clouds off)

         If running in single chip mode, you will also need the following options:
         rx: Raft x value 
         ry: Raft y value
         sx: Sensor x value
         sy: Sensor y value
         ex: Snap x value

Notes:   * You must have the LSST stack/eups setup - eg. source loadLSST.csh
           You must also, at a minimum, have the following LSST packages setup:
           pex_policy
           pex_logging
           pex_exceptions
           python (Minerva cluster python version is too old)

Methods:  1. __init__: setup and initialization of all needed parameters and directories.
          2. writeObsCatParams: make the object catalog parameter file
          3. generateAtmosphericParams: make the atmosphere parameter file.
          4. generateAtmosphericScreen: make the atmosphere screens.
          5. generateCloudScreen: make the cloud screens.
          6. generateControlParams: make the control parameter file.
          7. generateTrackingParams: make the tracking parameter file.
          8. loopOverChips: This is the main module. It runs the trim program
             to create catalogs for each chip. This method calls the following five methods.
          9. generateRaytraceParams: make the raytrace parameter file.
         10. generateBackgroundParams: make the background parameter file.
         11. generateCosmicRayParams: make the cosmic ray parameter file.
         12. generateE2adcParams: make the e2adc parameter files.
         13. makePbsScripts: make the PBs scripts, one for each sensor/snap with sources.
         14. cleanUp: move all parameter, pbs and tar files to save directory.

To Do:   Remove all directory dependence - use environment variables or policy file
         Need more robust verification of integrity of the SED directory on the node in the pbs script.
         Remove setup of pex* from batch script except in the case of Nicole's job monitor DB.
         
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

import ConfigParser
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


class SetupFullFocalplane:
    def __init__(self, trimfile, imsimPolicyFile, extraidFile, rx, ry, sx, sy, ex):
        
        """

        (0) Initialize the full field instance catalog parameters and
        the opsim catalog parameters.  Gather all needed parameters
        from opsim and set up various header keywords and parameter
        file names.  Create necessary working and save directories.

        """
        # Should not ever reference imsimsHomePath on exec node
        #self.imsimHomePath = os.getenv("IMSIM_HOME_DIR")
        self.imsimDataPath = os.getenv("CAT_SHARE_DATA")

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
        self.policy = ConfigParser.RawConfigParser()
        self.policy.read(imsimPolicyFile)
        # Job params
        self.numNodes = self.policy.get('general','numNodes')
        self.processors = self.policy.get('general','processors')
        self.pmem = self.policy.get('general','pmem')
        self.jobName = self.policy.get('general','jobname')
        # Directories and filenames
        self.scratchDataDir = self.policy.get('general','scratchDataDir')
        #self.scratchPath = self.policy.get('general','scratchPath')
        self.scratchOutputDir = self.policy.get('general','scratchOutputDir')
        self.savePath = self.policy.get('general','saveDir')
        self.stagePath = self.policy.get('general','stagingDir')

        #
        # PBS- and LSST-specific params
        #


        self.myrx = rx
        self.myry = ry
        self.mysx = sx
        self.mysy = sy
        self.myex = ex
            
        print 'Using instance catalog: ', self.trimfile
        print 'Your data directory is: ', self.scratchDataDir
        #print 'Your PBS username is: ', self.username
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
            if line.startswith('SIM_VISTIME'):
                name, self.vistime = line.split()
                print 'Sim_Vistime: ', self.vistime

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
        self.visitSavePath = os.path.join(self.savePath,'%s-f%s' %(self.obshistid, self.filter))
        self.logPath = os.path.join(self.visitSavePath, 'logs')
        self.paramDir = os.path.join(self.visitSavePath, 'run%s' %(self.obshistid))
        # NOTE: This might not be in the right location, but I never ran with self.centid==1.
        self.centroidPath = os.path.join(self.stagePath, 'imSim/PT1.2/centroid/v%s-f%s' %(self.obshistid, self.filter))
        if not os.path.isdir(self.logPath):
            try:
                os.makedirs(self.logPath)
            except OSError:
                pass
        print 'Your logfile directory is: ', self.logPath

        if not os.path.isdir(self.paramDir):
            try:
                os.makedirs(self.paramDir)
            except OSError:
                pass    
        print 'Your parameter directory is: ', self.paramDir
        
        if self.centid == '1':
            if not os.path.isdir(self.centroidPath):
                try:
                    os.makedirs(self.centroidPath)
                except OSError:
                    pass
            print 'Your centroid directory is %s' %(self.centroidPath)
 

        # Parameter File Names
        self.obsCatFile = 'objectcatalog_%s.pars' %(self.obshistid)
        self.obsParFile = 'obs_%s.pars' %(self.obshistid)
        self.atmoParFile = 'atmosphere_%s.pars' %(self.obshistid)
        self.atmoRaytraceFile = 'atmosphereraytrace_%s.pars' %(self.obshistid)
        self.cloudRaytraceFile = 'cloudraytrace_%s.pars' %(self.obshistid)
        self.opticsParFile = 'optics_%s.pars' %(self.obshistid)
        self.catListFile = 'catlist_%s.pars' %(self.obshistid)
        self.trackingParFile = 'tracking_%s.pars' %(self.obshistid)

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
        shutil.move('%s' %(self.atmoRaytraceFile), '../../')
        os.chdir('../../')
      
        return

    def generateAtmosphericScreen(self):
        
        """
        (3) Create the atmosphere screens.
        """
        
        print 'Generating the Atmospheric Screens.'
    
        os.chdir('ancillary/atmosphere')
        screenNumber = [0,1,2,3,4,5]
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
            
            shutil.move('%s_density.fits' %(atmoScreen), '../../')
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

    def loopOverChips(self, wav):
    
        """

        (7) Trim Program: Create the parameter files for each chip for
        each stage of the Simulator code then call the python code to
        create all of the PBS scripts for visit (378 PBS files will be
        generated).

        """
        # The SingleChipScriptGenerator class is designed so that only a single instance
        # needs to be called per execution of fullFocalPlane.py.  You can just call the
        # makeScript() method to create a script for each chip.
        scriptGen = SingleChipScriptGenerator_Pbs(self.policy, self.obshistid, self.filter,
                                                  self.filt, self.centid, self.centroidPath,
                                                  self.visitSavePath, self.paramDir,
                                                  self.trackingParFile)
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
                        parFile.write('trim \n')

                    print 'Running TRIM.'
                    os.chdir('ancillary/trim')
                    cmd = './trim < ../../%s' %(trimParFile)
                    subprocess.check_call(cmd, shell=True)
                    print 'Finished Running TRIM.'

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
                    try:
                        os.remove(trimParFile)
                    except:
                        #print 'WARNING: No file %s to remove!' %(trimParFile)
                        pass
                    
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
                            # if numLines > 0: # creates pbs file for the sensor designated by cid
                            if numLines > self.minsource:
                            
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
                                        parFile.write('chipid %s \n' %(cid))
                                        parFile.write('chipheightfile ../data/focal_plane/sta_misalignments/height_maps/%s.fits.gz \n' %(cid))                                        

                                    raytraceParFile   = parFactory.raytrace(self.obshistid, id)
                                    backgroundParFile = parFactory.background(self.obshistid, id)
                                    cosmicParFile     = parFactory.cosmic(self.obshistid, id)
                                    # GENERATE THE RAYTRACE PARS
                                    print 'Running the raytrace.'
                                    generateRaytraceParams(self, id, chipParFile, seedchip, timeParFile,
                                                           raytraceParFile)
                                    # GENERATE THE BACKGROUND ADDER PARS
                                    print 'Running the background.'
                                    generateBackgroundParams(self, id, seedchip, cid, wav, backgroundParFile)
                                    # GENERATE THE COSMIC RAY ADDER PARS
                                    print 'Running cosmic rays.'
                                    generateCosmicRayParams(self, id, seedchip, cosmicParFile)
                                    # GENERATE THE ELECTRON TO ADC CONVERTER PARS
                                    print 'Running e2adc.'
                                    generateE2adcParams(self, id, cid, ex, rx, ry, sx, sy)
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
                                                             backgroundParFile, cosmicParFile, sensorId)
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

    axList = ['0', '1']
    ayList = ['0', '1', '2', '3', '4', '5', '6', '7']
    for ax in axList:
        for ay in ayList:
            nrx = int(rx) * 1440
            nry = int(ry) * 288
            nsx = int(sx) * 96
            nsy = int(sy) * 32
            nax = int(ax) * 16
            nay = int(ay) * 2
            seedamp = int(self.obsid) + nrx + nry + nsx + nsy + nax + nay + int(ex)
            eid = 'R'+rx+ry+'_S'+sx+sy+'_C'+ax+ay+'_E00'+ex
            e2adcParFile = 'e2adc_%s_%s.pars' %(self.obshistid, eid)

            try:
                os.remove(e2adcParFile)
            except:
                #print 'WARNING: No file %s to remove!' %(e2adcParFile)
                pass

            cmd = 'cat data/focal_plane/sta_misalignments/offsets/pars_%s_C%s%s data/focal_plane/sta_misalignments/readout/readoutpars_R%s%s_S%s%s_C%s%s >> %s' %(cid, ax, ay, rx, ry, sx, sy, ax, ay, e2adcParFile)
            subprocess.check_call(cmd, shell=True)
            
            with file(e2adcParFile, 'a') as parFile:
                parFile.write('inputfilename ../cosmic_rays/output_%s_%s.fits.gz \n' %(self.obshistid, id))
                parFile.write('outputfilename imsim_%s_%s \n' %(self.obshistid, eid) )
                parFile.write('chipid %s \n' %(cid))
                parFile.write('chipoutid R%s%s_S%s%s_C%s%s \n' %(rx, ry, sx, sy, ax, ay))
                parFile.write('qemapfilename ../../data/focal_plane/sta_misalignments/qe_maps/QE_R%s%s_S%s%s.fits.gz \n' %(rx, ry, sx, sy))
                parFile.write('exptime %s \n'%(self.exptime)) 
                parFile.write('seed %s \n' %(seedamp))
                parFile.write('e2adc \n')

def cleanUp(self):
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

        os.chdir('%s' %(self.workDir))
        # Files needed for ancillary/atmosphere
        print 'Tarring atmosphere files.'
        cmd = 'tar cvf nodeFiles%s.tar atmospherescreen_%s_*.fits cloudscreen_%s_*.fits cloudraytrace_%s.pars control_%s.pars objectcatalog_%s.pars' %(self.obshistid, self.obshistid, self.obshistid, self.obshistid, self.obshistid, self.obshistid)
        subprocess.check_call(cmd, shell=True)

        # Files for ancillary/Add_Background
        print 'Tarring ancillary/Add_Background/ files.'
        shutil.move('nodeFiles%s.tar' %(self.obshistid), 'ancillary/Add_Background')
        os.chdir('ancillary/Add_Background')
        shutil.copy('SEDs/darksky_sed.txt', '.')
        shutil.copy('SEDs/lunar_sed.txt', '.')
        shutil.copy('SEDs/sed_dome.txt', '.')
        cmd = 'tar rvf nodeFiles%s.tar filter_constants darksky_sed.txt lunar_sed.txt sed_dome.txt filter_constants_dome' %(self.obshistid)
        subprocess.check_call(cmd, shell=True)
        os.remove('darksky_sed.txt')
        os.remove('lunar_sed.txt')
        os.remove('sed_dome.txt')

        # Executables and binaries files for running on nodes.
        print 'Tarring binaries/executables.'
        shutil.move('nodeFiles%s.tar' %(self.obshistid), '../../')
        os.chdir('../../')
        cmd = 'tar rvf nodeFiles%s.tar ancillary/trim/trim ancillary/Add_Background/* ancillary/cosmic_rays/* ancillary/e2adc/e2adc raytrace/lsst raytrace/*.txt  raytrace/version raytrace/setup pbs/distributeFiles.py chip.py' %(self.obshistid)
        subprocess.check_call(cmd, shell=True)

        # Zip the tar file.
        print 'Gzipping nodeFiles%s.tar file' %(self.obshistid)
        cmd = 'gzip nodeFiles%s.tar' %(self.obshistid)
        subprocess.check_call(cmd, shell=True)
        print 'Moving nodeFiles%s.tar.gz to %s/.' %(self.obshistid, self.visitSavePath)
        shutil.move('nodeFiles%s.tar.gz' %(self.obshistid), '%s/' %(self.visitSavePath)) 

    # Move the parameter, pbs, and fits files to the run directory.
    print 'Moving run files to %s directory.' %(self.paramDir)
    for fits in glob.glob('*.fits'):
        shutil.move(fits, '%s' %(self.paramDir))

    for pars in glob.glob('*.pars'):
        shutil.move(pars, '%s' %(self.paramDir))

    if self.myrx == '':
        # Deal with the pbs and command files if created (not single chip mode).
        for pbs in glob.glob('pbs_%s_*.pbs' %(self.obshistid)):
            shutil.move(pbs, '%s' %(self.paramDir))

        try:
            for cmds in glob.glob('cmds_*.txt'):
                os.remove(cmds)
        except:
            #print 'WARNING: No command files to remove!'
            pass

        try:
            cmd = 'ls %s/*.pbs > %s_f%sPbsJobs.lis' %(self.paramDir, self.obshistid, self.filter)
            subprocess.check_call(cmd,shell=True)
            print 'Created qsub list.'
            print 'Finished tarring and moving files.  Ready to launch PBS scripts.'
        except:
            print 'WARNING: No PBS files to list!'
            pass
    
    return



def main(trimfile, imsimPolicyFile, extraidFile, rx, ry, sx, sy, ex):

    """

    Run the fullFocalplanePbs.py script, populating it with the
    correct user and cluster job submission information from an LSST
    policy file. 
    
    """

    print 'Running SetupFocalplane on: ', trimfile
    x = SetupFullFocalplane(trimfile, imsimPolicyFile, extraidFile, rx, ry, sx, sy, ex)      
    SetupFullFocalplane.writeObsCatParams(x)
    SetupFullFocalplane.generateAtmosphericParams(x)
    wav = SetupFullFocalplane.generateAtmosphericScreen(x)
    SetupFullFocalplane.generateCloudScreen(x)
    SetupFullFocalplane.generateControlParams(x)
    SetupFullFocalplane.generateTrackingParams(x)
    SetupFullFocalplane.loopOverChips(x, wav)
    cleanUp(x)
    
if __name__ == "__main__":

    if not len(sys.argv) == 9:
        print "usage: python fullFocalplane.py trimfile imsimConfigFile extraidFile rx ry sx sy ex"
        quit()

    trimfile = sys.argv[1]
    imsimConfigFile = sys.argv[2]
    extraidFile = sys.argv[3]
    rx = sys.argv[4]
    ry = sys.argv[5]
    sx = sys.argv[6]
    sy = sys.argv[7]
    ex = sys.argv[8]

    main(trimfile, imsimConfigFile, extraidFile, rx, ry, sx, sy, ex)
