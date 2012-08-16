#!/usr/bin/python2.6
import os
import unittest
from AllChipsScriptGenerator import *
from optparse import OptionParser

class MockAllChipsScriptGenerator(AllChipsScriptGenerator):
  def _loadFocalplaneNames(self, extraidFile, extraid, centid):
    self.obshistid = '1234560'
    self.filterNum = '2'
    self.filterName = filterToLetter(self.filterNum)
    self.extraid = '0'
    self.centid = centid
    return

  def _makePaths(self):
    return


class MockAllChipsScriptGenerator_Pbs(AllChipsScriptGenerator_Pbs):
  def _loadFocalplaneNames(self, extraidFile, extraid, centid):
    self.obshistid = '1234560'
    self.filterNum = '2'
    self.filterName = filterToLetter(self.filterNum)
    self.extraid = '0'
    self.centid = centid
    return

  def _makePaths(self):
    return


class TestAllChipsScriptGenerator(unittest.TestCase):

  def _SetupWorkstation(self):
    self.baseDir = os.path.dirname(__file__)
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_workstation.cfg')
    self.extraidFile = os.path.join(self.baseDir, 'clouds')
    self.scheduler = 'csh'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)
    self.rx = 2
    self.ry = 2
    self.sx = 2
    self.sy = 2
    self.ex = 1


  def test_InitWorkstation(self):
    """
    Tests the import and initialization of AllChipsScriptGenerator class.  Initialization
    includes parsing the .cfg file
    """
    self._SetupWorkstation()
    # This will raise an exception if it does not read in the config file
    s = MockAllChipsScriptGenerator('mockTrimFile', self.policy, self.extraidFile)

    # This variable will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    self.assertEquals(s.trackingParFile, 'tracking_1234560.pars')

  def _SetupPbs(self):
    self._SetupWorkstation()
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_minerva.cfg')
    self.scheduler = 'pbs'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitPbs(self):
    self._SetupPbs()
    # This will raise an exception if it does not read in the config file
    s = MockAllChipsScriptGenerator_Pbs('mockTrimFile', self.policy, self.extraidFile)

    # These variables will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    try:
      s.username
    except:
      raise
    self.assertEquals(s.trackingParFile, 'tracking_1234560.pars')

if __name__ == '__main__':
    unittest.main()
