#!/usr/bin/python
"""
Classes that manage a single ImSim single-chip exposure

Notation: For naming the rafts, sensors, amplifiers, and exposures, we
          obey the following convention:
            raftid:   Raft ID string of the form 'R[0-4][0-4]'
            sensorid: Sensor ID string of the form 'S[0-2][0-2]'
            expid:    Exposure ID string of the form 'E[0-9][0-9][0-9]'
            cid:      Chip/sensor ID string of the form '<raftid>_<sensorid>'
            ampid:    Amplifier ID string of the form '<cid>_C[0-1][0-7]'
            id:       Full Exposure ID string of the form '<cid>_<expid>'
            obshistid: ID of the observation from the trim file with the 'extraID'
                       digit appended ('clouds'=0, 'noclouds'=1).
"""
from __future__ import with_statement
import os, re, sys
import subprocess


def filterToLetter(filterIdNum):
    """Convert numeric filter ID to alphabetic."""
    filtmapToLetter = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
    return filtmapToLetter[filterIdNum]


def filterToNumber(filterIdName):
    """Convert alphabetic filter ID to numeric."""
    filtmapToNumber = {"u":"0", "g":"1", "r":"2", "i":"3", "z":"4", "y":"5"}
    return filtmapToNumber[filterIdName]


def findSourceFile(filename):
    """Locates a file that is in the source tree.

    Searches for the file in both the cwd and in IMSIM_SOURCE_PATH.

    Args:
      filename:  Should include the relative path of file from these locations,
                 e.g. 'lsst/segmentation.txt'

    Returns:
      The full path of the file.
    """
    if not os.path.isfile(filename):
        #print filename, 'does not exist in cwd.  Checking IMSIM_SOURCE_PATH.'
        imsimSourcePath = os.getenv("IMSIM_SOURCE_PATH")
        if imsimSourcePath is None:
            raise NameError('Could not find value for IMSIM_SOURCE_PATH needed to read %s.'
                            % filename)
        filename = os.path.join(imsimSourcePath, filename)
        if not os.path.isfile(filename):
            raise RuntimeError('Could not %s' %filename)
    #print 'Found', filename
    return filename

def idStringsFromFilename(filename):
    """Determines the obshistid and the full exposure id from filename.

    Filename must be structured as
    name_<obshistid>_<rafid>_<sourceid>_<expid>.extension

    Args:
    filename:   Name of the file to parse

    Returns:
    obshistid:  obshistid
    id:         full exposure id of the form <raftid>_<sourceid>_<expid>
    """
    tokens = filename.split('_')
    assert len(tokens) == 5
    obshistid = tokens[1]
    id = '%s_%s_%s' %(tokens[2], tokens[3], tokens[4].split('.')[0])
    return obshistid, id


def readAmpList(ampFile, cid):
    """Reads list of amps for a specific chip.

    Args:
      ampFile:  pointer to file containing amp list
      cid:      chipid to load

    Return:
      list of amp names
    """
    ampList = []
    for line in ampFile.readlines():
        if line.startswith('%s_' %cid):
            ampList.append(line.split()[0])
    return ampList


def verifyFileExistence(missingList, path, filename):
    """Verifies the existing of 'path/filename'.

    Args:
      missingList:  List of missing files to which this file will
                    be appended if missing.
      path:         File path not including name.
      filename:     File name.

    Returns:
      True if file exists, false otherwise.
    """
    fullpath = os.path.join(path, filename)
    if not os.path.isfile(fullpath):
        missingList.append(fullpath)
        return False
    return True


def verifyFitsContents(corruptList, path, filename):
    """Runs fitsverify on a FITS file to verify contents.

    Args:
      corruptList:  A lits of corrupt files to which this file will
                    be appended if corrupt.
      path:         File path not including name.
      filename:     File name.

    Returns:
      True if fitsverify returns OK, false otherwise.
    """
    fullpath = os.path.join(path, filename)
    # If the fitsverify executable is in the cwd, use that. Otherwise,
    # use the copy in the path.  This is necessary in case the cwd
    # is not in the path.
    if os.path.isfile("fitsverify"):
        cmd = "./fitsverify"
    else:
        cmd = "fitsverify"
    p = subprocess.Popen("%s -q -e %s"%(cmd, fullpath), shell=True,
                         stdout=subprocess.PIPE, close_fds=True)
    output = p.stdout.readlines()[0]
    p.stdout.close()
    if output.startswith("verification OK"):
        return True
    corruptList.append((fullpath, output))
    return False


class Exposure(object):
    def __init__(self, obshistid, filterid, id):
        self.obshistid = obshistid
        self.filterName = filterid
        self.filterNum = filterToNumber(self.filterName)
        self.id = id
        self.raftid, self.sensorid, self.expid = self.id.split("_")
        self.cid = "%s_%s" %(self.raftid, self.sensorid)
        self.ampList = []

    def generateEimageExecName(self):
        return 'eimage_%s_f%s_%s.fits.gz' %(self.obshistid, self.filterNum, self.id)

    def generateEimageOutputName(self):
        path = "eimage/v%08d-f%s/%s/%s" % (int(self.obshistid), self.filterName,
                                          self.expid, self.raftid)
        name = "eimage_%s_%s_%s_%s.fits.gz"%(self.obshistid, self.raftid, self.sensorid, self.expid)
        return path, name

    def generateRawExecNames(self):
        self._loadAmpList()
        names = []
        for ampid in self.ampList:
            names.append('imsim_%s_f%s_%s_%s.fits.gz' %(self.obshistid, self.filterNum, ampid, self.expid))
        return names

    def generateRawOutputNames(self):
        self._loadAmpList()
        path = "raw/v%08d-f%s/%s/%s/%s" % (int(self.obshistid), self.filterName,
                                          self.expid, self.raftid, self.sensorid)
        names = []
        for ampid in self.ampList:
            names.append("imsim_%s_%s_%s.fits.gz"%(self.obshistid, ampid, self.expid))
        return path, names

    def _loadAmpList(self):
        if not self.ampList:
            with open(findSourceFile('lsst/segmentation.txt'), 'r') as ampFile:
                self.ampList = readAmpList(ampFile, self.cid)
        assert self.ampList
        return

    def verifyExecFiles(self, outputDir):
        missingList = []
        corruptList = []
        name = self.generateEimageExecName()
        if verifyFileExistence(missingList, outputDir, name):
            verifyFitsContents(corruptList, outputDir, name)
        names = self.generateRawExecNames()
        for name in names:
            if verifyFileExistence(missingList, outputDir, name):
                verifyFitsContents(corruptList, outputDir, name)
        return missingList, corruptList

    def verifyOutputFiles(self, outputPath):
        missingList = []
        path, name = self.generateEimageOutputName()
        verifyFileExistence(missingList, os.path.join(outputPath, path), name)
        path, names = self.generateRawOutputNames()
        for name in names:
            verifyFileExistence(missingList, os.path.join(outputPath, path), name)
        return missingList
