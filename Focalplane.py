#!/usr/bin/python
"""
Classes that manage a single ImSim full focal plane
"""

from __future__ import with_statement
import datetime
import logging
import math
import shutil
import subprocess
import time
import os, re, sys
from Exposure import verifyFileExistence
from Exposure import idStringsFromFilename
from Exposure import filterToLetter
from Exposure import findSourceFile

def readCidList(camstr, fplFile):
    """Search focalplanelayout file for lines matching the regex in camstr.

    Args:
      camstr:   camconfig regex
      fplFile:  pointer to focalplanelayout file

    Returns:
      List of 3-tuples from matchiing lines: (cid, devtype, float(devvalue))
    """
    cidList = []
    p = re.compile(camstr)
    for line in fplFile.readlines():
        if p.search(line):
            c = line.split()
            cidList.append( (c[0],c[6],float(c[7])) )
    return cidList

def generateRaytraceJobManifestFilename(obshistid, filter):
  return '%s-f%s-Jobs.lis' %(obshistid, filter)

class ParsFilenames(object):
    """
    A simple class to try to keep all the filename definitions in one place.
    key:
      ex  = 0 or 1
      id  = 'R'+rx+ry+'_'+'S'+sx+sy+'_'+'E00'+ex

    """
    def __init__(self, _obshistid):
      self.obshistid = _obshistid
      return

    def time(self, expid):
        return 'time_%s_%s.pars' %(self.obshistid, expid)

    def chip(self, id):
        return 'chip_%s_%s.pars' %(self.obshistid, id)

    def raytrace(self, id):
        return 'raytracecommands_%s_%s.pars' %(self.obshistid, id)

    def background(self, id):
        return 'background_%s_%s.pars' %(self.obshistid, id)

    def cosmic(self, id):
        return 'cosmic_%s_%s.pars' %(self.obshistid, id)

    def e2adc(self, id):
        return 'e2adc_%s_%s.pars' %(self.obshistid, id)

    def sedlist(self, cid):
        return 'sedlist_%s_%s.txt' %(self.obshistid, cid)

    def trimcatalog(self, id):
        return 'trimcatalog_%s_%s.pars.gz' %(self.obshistid, id)

class FileVerifyError(Exception):
    """Base class for exceptions in this module."""
    pass

class FileNotFoundError(FileVerifyError):
    """Could not find file.
    Attributes:
       filename:  Name of file
    """
    def __init__(self, filename):
        self.filename = filename

class FileSizeError(FileVerifyError):
    """File is not above a size threshold
    Attributes:
       filename:       Name of file
       filesize(int):  Size of file in bytes
       minsize(int):   Minimum size of file
    """
    def __init__(self, filename, filesize, minsize):
        self.filename = filename
        self.filesize = filesize
        self.minsize = minsize


class WithTimer:
    """http://preshing.com/20110924/timing-your-code-using-pythons-with-statement"""
    def __enter__(self):
        self.startCpu = time.clock()
        self.startWall = time.time()
        return self

    def __exit__(self, *args):
        self.interval = []
        self.interval.append(time.clock() - self.startCpu)
        self.interval.append(time.time() - self.startWall)

    def Print(self, name, stream):
      stream.write('TIMER[%s]: cpu: %f sec  wall: %f sec\n' %(name, self.interval[0],
                                                              self.interval[1]))

    def PrintCpu(self, name, stream):
      stream.write('TIMER[%s]: cpu: %f sec\n' %(name, self.interval[0]))

    def PrintWall(self, name, stream):
      stream.write('TIMER[%s]: wall: %f sec\n' %(name, self.interval[1]))

    def Log(self, name):
      logging.info('TIMER[%s]: cpu: %f sec  wall: %f sec\n', name,
                   self.interval[0], self.interval[1])

    def LogCpu(self, name):
      logging.info('TIMER[%s]: cpu: %f sec\n', name, self.interval[0])

    def LogWall(self, name):
      logging.info('TIMER[%s]: wall: %f sec\n', name, self.interval[1])


class Focalplane(object):

    def __init__(self, obshistid, filterName):
        """Constructor.

        NOTE: obsid = <obshistid>-f<filterName>

        Args:
          obshistid:  obshistid
          filterName: Alphabetic filter ID
        """
        self.obshistid = obshistid
        self.filterName = filterName
        self.obsid = '%s-f%s' %(self.obshistid, self.filterName)
        self.trimfileName = None

        # Dictionary of Parameter Filenames
        _d = {}
        _d['objectcatalog']  = 'objectcatalog_%s.pars' %(self.obshistid)
        _d['obs']            = 'obs_%s.pars' %(self.obshistid)
        _d['atmosphere']     = 'atmosphere_%s.pars' %(self.obshistid)
        _d['atmosphereraytrace'] = 'atmosphereraytrace_%s.pars' %(self.obshistid)
        _d['cloudraytrace']  = 'cloudraytrace_%s.pars' %(self.obshistid)
        _d['control']        = 'control_%s.pars' %(self.obshistid)
        _d['optics']         = 'optics_%s.pars' %(self.obshistid)
        _d['catlist']        = 'catlist_%s.pars' %(self.obshistid)
        _d['tracking']       = 'tracking_%s.pars' %(self.obshistid)
        _d['track']          = 'track_%s.pars' %(self.obshistid)
        self.parsDictionary = _d
        self.cidList = []
        self.camstr = ''
        self.idonly = ''
        # Parameter file names for compatability with Nicole's functions
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


    def generateCidList(self, camstr="", idonly=""):
        """Generate list of chipIDs for this telescope.

        Args:
          camstr:    Supply camstr other than what was calculated when trimfile
                     was loaded.
          idonly:    Optional exposure id.  Only process the specified exposure.
                     (provided for backwards compatability with Nicole's script)

        Returns:
          List of cid strings.
        """
        if not camstr:
            assert self.camstr
            camstr = self.camstr
        self._loadCidList(camstr, idonly)
        return self.cidList

    def _loadCidList(self, camstr, idonly):
        if self.cidList:
            return
        if idonly:
            self.idonly = idonly
        if self.idonly:
            raftid, sensorid, expid = self.idonly.split("_")
            self.cidList = ('R%s_S%s' %(raftid, sensorid),
                            'CCD', 3.0)
        else:
            fplFilename = findSourceFile('lsst/focalplanelayout.txt')
            with open(fplFilename, 'r') as f:
                self.cidList = readCidList(camstr, f)
        return

    def idListFromExecFiles(self, paramPath, in_id_list):
        """
        Generates a list of exposure IDs for each exec_* file in
        stagePath2. If the input _exp_list is not empty,
        it will restrict this search to just those exposure IDs given
        in in_id_list.
        """
        all_files = os.listdir(paramPath)
        id_list = []
        for filename in all_files:
            if filename.split("_")[0] != 'exec':
                continue
            obshistid, id = idStringsFromFilename(filename)
            if obshistid != self.obshistid:
                continue
            if in_id_list:
                # Check if this is an element in in_id_list.
                # TODO(gardnerj) make this search more efficient someday.
                for i in in_id_list:
                    if id == i:
                        id_list.append(id)
            else:
                id_list.append(id)
        return id_list

    def verifyInputFiles(self, stagePathRoot, idlist=""):
        missingList = []
        stagePath = os.path.join(stagePathRoot, self.obsid)
        nodeFilesTgz = 'nodeFiles%s.tar.gz' %self.obshistid
        verifyFileExistence(missingList, stagePath, nodeFilesTgz)
        paramPath = os.path.join(stagePath, 'run%s' %self.obshistid)
        for k,v in self.parsDictionary.iteritems():
            verifyFileExistence(missingList, paramPath, v)
        idsToVerify = self.idListFromExecFiles(paramPath, idlist)
        pfn = ParsFilenames(self.obshistid)
        for id in idsToVerify:
            if idlist:
                print 'Checking files for id=%s' %id
            Rxx, Sxx, expid = id.split('_')
            cid = '%s_%s' %(Rxx, Sxx)
            verifyFileExistence(missingList, paramPath, pfn.time(expid))
            verifyFileExistence(missingList, paramPath, pfn.chip(id))
            verifyFileExistence(missingList, paramPath, pfn.raytrace(id))
            verifyFileExistence(missingList, paramPath, pfn.background(id))
            verifyFileExistence(missingList, paramPath, pfn.cosmic(id))
            verifyFileExistence(missingList, paramPath, pfn.e2adc(id))
            verifyFileExistence(missingList, paramPath, pfn.sedlist(cid))
            verifyFileExistence(missingList, paramPath, pfn.trimcatalog(id))
        return missingList

    def loadTrimfile(self, trimfileName):
        """Loads trimfile metadata and calculates useful params.

        Also reads default_instcat before trimfile.

        Args:
          trimfileName:  Name of trimfile
        """
        # Read in the default catalog first, then replace the non-default
        # values with the actual trimfile
        print 'Using instance catalog: default_instcat',
        print '***'
        with open('default_instcat','r') as trimfile:
            self._readTrimfile(trimfile)
        print 'Using instance catalog: ', trimfileName
        print '***'
        with open(trimfileName,'r') as trimfile:
            self._readTrimfile(trimfile)
        self._calculateParams()
        self.trimfileName = trimfileName
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

    def _readTrimfile(self, trimfile):
        print 'Initializing Opsim and Instance Catalog Parameters.'
        for line in trimfile:
            if line.startswith('SIM_SEED'):
                name, self.simseed = line.split()
                print 'SIM_SEED:', self.simseed
            elif line.startswith('Unrefracted_RA'):
                name, self.pra = line.split()
                print 'Unrefracted_RA:', self.pra
            elif line.startswith('Unrefracted_Dec'):
                name, self.pdec = line.split()
                print 'Unrefracted_Dec:', self.pdec
            elif line.startswith('Opsim_moonra'):
                name, self.mra = line.split()
                print 'Opsim_moonra:', self.mra
            elif line.startswith('Opsim_moondec'):
                name, self.mdec = line.split()
                print 'Opsim_moondec:', self.mdec
            elif line.startswith('Opsim_rotskypos'):
                name, self.prot = line.split()
                print 'Opsim_rotskypos:', self.prot
            elif line.startswith('Opsim_rottelpos'):
                name, self.spid = line.split()
                print 'Opsim_rottelpos:', self.spid
            elif line.startswith('Opsim_filter'):
                name, self.filterNum = line.split()
                print 'Opsim_filter:', self.filterNum
            elif line.startswith('Unrefracted_Altitude'):
                name, self.alt = line.split()
                print 'Unrefracted_Altitude:', self.alt
            elif line.startswith('Unrefracted_Azimuth'):
                name, self.az = line.split()
                print 'Unrefracted_Azimuth:', self.az
            elif line.startswith('Opsim_rawseeing'):
                name, self.rawseeing = line.split()
                print 'Opsim_rawseeing:', self.rawseeing
            elif line.startswith('Opsim_sunalt'):
                name, self.sunalt = line.split()
                print 'Opsim_sunalt:', self.sunalt
            elif line.startswith('Opsim_moonalt'):
                name, self.moonalt = line.split()
                print 'Opsim_moonalt:', self.moonalt
            elif line.startswith('Opsim_dist2moon'):
                name, self.moondist = line.split()
                print 'Opsim_dist2moon:', self.moondist
            elif line.startswith('Opsim_moonphase'):
                name, self.moonphase = line.split()
                print 'Opsim_moonphase:', self.moonphase
            elif line.startswith('Slalib_date'):
                name, stringDate = line.split()
                year, self.month, day, time = stringDate.split('/')
                print 'Slalib_date:', self.month
            elif line.startswith('Opsim_obshistid'):
                name, obshistid = line.split()
                print 'Opsim_obshistid: ', obshistid
                # Don't reload self.obshistid since it won't have extraid in it.
                # obshistid == 9999 in default_instcat
                assert obshistid == '9999' or obshistid in self.obshistid
            elif line.startswith('Opsim_expmjd'):
                name, self.tai = line.split()
                print 'Opsim_expmjd: ', self.tai
            elif line.startswith('SIM_MINSOURCE'):
                name, self.minsource = line.split()
                # if SIM_MINSOURCE = 0 - images will be generated for
                # chips with zero stars on them (background images)
                print 'Sim_Minsource: ', self.minsource
            elif line.startswith('SIM_TELCONFIG'):
                name, self.telconfig = line.split()
                print 'Sim_Telconfig: ', self.telconfig
            elif line.startswith('SIM_CAMCONFIG'):
                self.camconfig = int(line.split()[1])
                print 'Sim_Camconfig: ', self.camconfig
            elif line.startswith('SIM_VISTIME'):
                self.vistime = float(line.split()[1])
                print 'Sim_Vistime: ', self.vistime
            elif line.startswith('SIM_NSNAP'):
                self.nsnap = int(line.split()[1])
                print 'Sim_Nsnap: ', self.nsnap
            elif line.startswith('isDithered'):
                self.isDithered = int(line.split()[1])
                print 'isDithered: ', self.isDithered
            elif line.startswith('ditherRaOffset'):
                self.ditherRaOffset = float(line.split()[1])
                print 'ditherRaOffset: ', self.ditherRaOffset
            elif line.startswith('ditherDecOffset'):
                self.ditherDecOffset = float(line.split()[1])
                print 'ditherDecOffset: ', self.ditherDecOffset
        return

    def runPreprocessingCommands(self, trimfile=None, camstr='', idonly=''):
        """Perform all preprocessing steps that are common to entire focalplane.

        This is the main worker routine.  It just goes through and calls
        all of Nicole's original functions.

        Args:
          trimfile:  Name of trimfile.  Only required If trimfile has not been
                     loaded already.
          camstr:    Supply camstr other than what was calculated when trimfile
                     was loaded.
          idonly:    Optional exposure id.  Only process the specified exposure.
                     (provided for backwards compatability with Nicole's script)

        Returns:
          (float)wavelength returned from generateAtmosphereScreen
        """
        if idonly:
            self.idonly = idonly
        if self.trimfileName is None:
          if trimfile is None:
            raise RuntimeError('"trimfile" must be supplied if it has not been loaded')
          self.loadTrimfile(trimfile)
          assert self.trimfileName == trimfile
        self.writeObsCatParams()
        self.generateAtmosphericParams()
        wav = self.generateAtmosphericScreen()
        self.generateCloudScreen()
        self.generateControlParams()
        self.generateTrackingParams()
        # cidlist required in generateTrimCatalog
        self.generateCidList(camstr, idonly)
        self.generateTrimCatalog()
        return wav

    def writeObsCatParams(self):
        """
        (1) Write the parameters from the object catalog file to the
        obs parameter file.  This section is referred to as
        'Constructing a Catalog List' in John's full_focalplane shell
        script, most of which is new for PT1.2.

        """
        assert self.trimfileName is not None
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

        cmd = 'grep object %s %s >> %s' %(self.trimfileName, objectTestFile, self.obsCatFile)
        subprocess.check_call(cmd, shell=True)

        numlines = len(open(self.obsCatFile).readlines())
        print 'numlines', numlines
        if numlines > 1:
            try:
                os.remove(self.obsCatFile)
            except:
                #print 'WARNING: No file %s to remove!' %(self.obsCatFile)
                pass
            cmd = 'grep object %s >> %s' %(self.trimfileName, self.obsCatFile)
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

        cmd = ('grep includeobj %s %s' %(self.trimfileName, includeObjFile))
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        results = p.stdout.readlines()
        p.stdout.close()
        nincobj = len(results)

        if nincobj > 1:
            cmd = ('grep includeobj %s | awk -v x=%i \'{print "catalog",x,"../../"$2};{x+=1}\' >> %s' %(self.trimfileName, self.ncat, self.catListFile))
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
            parFile.write('filter %s \n' %(self.filterNum))
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
        assert self.trimfileName is not None
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
            parFile.write('seed %s \n'%(self.simseed))
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
        assert self.trimfileName is not None
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
            if self.filterNum == '0':
                wav = 0.36
            elif self.filterNum == '1':
                wav = 0.48
            elif self.filterNum == '2':
                wav = 0.62
            elif self.filterNum == '3':
                wav = 0.76
            elif self.filterNum == '4':
                wav = 0.87
            else:
                wav = 0.97
            atmoScreen = 'atmospherescreen_%s_%s' %(self.obshistid, screen)
            cmd = 'time ./turb2d -seed %s%s -see5 %s -outerx 50000.0 -outers %s -zenith %s -wavelength %s -name %s' %(self.simseed, screen, self.rawseeing, low, self.zen, wav, atmoScreen)
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
        assert self.trimfileName is not None
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
            cmd = 'time ./cloud -seed %s%s -height %s -name %s -pix 100' %(self.simseed, screen, height, cloudScreen)
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
            parFile.write('ranseed %s \n' %(self.simseed))
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
            parFile.write('seed %s \n' %(self.simseed))
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
        (6.9)
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
        (7)
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

    def generateRaytraceParams(self, id, chipParFile, seedchip, timeParFile, raytraceParFile,
                               extraidFilename=''):

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
        if extraidFilename:
            cmd = 'cat %s >> %s' %(extraidFilename, chipParFile)
            subprocess.check_call(cmd, shell=True)

        cmd = 'cat %s %s %s %s %s %s > %s' %(self.obsParFile, self.atmoRaytraceFile,
                                             self.opticsParFile, timeParFile,
                                             self.cloudRaytraceFile, chipParFile,
                                             raytraceParFile)
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
            parFile.write('filter %s \n' %(self.filterNum))
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
