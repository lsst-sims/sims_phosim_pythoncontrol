#!/usr/bin/python
"""
Classes that manage a single ImSim full focal plane
"""

from __future__ import with_statement
import os, re, sys
import chip
from Exposure import verifyFileExistence

def readCidList(camstr, fplFile):
    """Search the focalplanelayout file for lines in fplFile matching
    the regex in camstr.  Return a list from the matching lines.
    Each list element is a tuple (cid, devtype, float(devvalue))
    """
    cidList = []
    p = re.compile(camstr)
    for line in fplFile.readlines():
        if p.search(line):
            c = line.split()
            cidList.append( (c[0],c[6],float(c[7])) )
    return cidList

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

class Focalplane(object):
    def __init__(self, obshistid, filterid):
        """
        NOTE: obsid = <obshistid>-f<filter>
              I should call this focalplaneId, but I don't want to get
              confused with exacycle where I build in campaign ids and
              timestamps and all that.
        """
        self.obshistid = obshistid
        self.filter = filterid
        self.obsid = '%s-f%s' %(self.obshistid, self.filter)

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
        self.camstr = ""
    

    def _loadCidList(self, camstr, idonly):
        if not self.cidList:
            if idonly:
                raftid, sensorid, expid = self.idonly.split("_")
                self.cidList = ('R%s_S%s' %(raftid, sensorid),
                                'CCD', 3.0)
            else:
                fplFilename = chip.findSourceFile('lsst/focalplanelayout.txt')
                with open(fplFilename, 'r') as f:
                    self.cidList = readCidList(camstr, f)
        return

        
    def generateCidList(self, camstr="", idonly=""):
        if not camstr:
            camstr = self.camstr
        assert camstr
        self._loadCidList(camstr, idonly)
        return self.cidList
        
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

    def idListFromExecFiles(self, paramPath, in_exp_list):
        """
        Generates a list of exposure IDs for each exec_* file in
        stagePath2. If the input _exp_list is not empty,
        it will restrict this search to just those exposure IDs given
        in in_exp_list.
        """
        exec_list = os.listdir(paramPath)
        exp_list = []
        for exec_filename in exec_list:
            e = exec_filename.split('_')
            if e[0] != 'exec' or e[1] != self.obshistid:
                continue
            expid = '%s_%s_%s' %(e[2], e[3], e[4].split('.')[0])
            if in_exp_list:
                # Check if this is an element in in_exp_list.
                # TODO(gardnerj) make this search more efficient someday.
                for i in in_exp_list:
                    if expid == i:
                        exp_list.append(expid)
            else:
                exp_list.append(expid)
        return exp_list

    
