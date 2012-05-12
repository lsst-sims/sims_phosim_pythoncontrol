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
import chip

def verifyFileExistence(missingList, path, filename):
    fullpath = os.path.join(path, filename)
    if not os.path.isfile(fullpath):
        missingList.append(fullpath)
        return False
    return True

def verifyFitsContents(corruptList, path, filename):
    fullpath = os.path.join(path, filename)
    p = subprocess.Popen("fitsverify -q -e %s"%fullpath, shell=True,
                              stdout=subprocess.PIPE, close_fds=True)
    output = p.stdout.readlines()[0]
    p.stdout.close()
    if output.startswith("verification OK"):
        return True
    corruptList.append((fullpath, output))
    return False

class Exposure(object):
    def __init__(self, obshistid, filterid, id):
        self.filtmapToLetter = {"0":"u", "1":"g", "2":"r", "3":"i", "4":"z", "5":"y"}
        self.filtmapToNumber = {"u":"0", "g":"1", "r":"2", "i":"3", "z":"4", "y":"5"}
        self.obshistid = obshistid
        self.filterName = filterid
        self.filterNum = self.filtmapToNumber[self.filterName]
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
            with open(chip.findSourceFile('lsst/segmentation.txt'), 'r') as ampFile:
                self.ampList = chip.readAmpList(ampFile, self.cid)
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

