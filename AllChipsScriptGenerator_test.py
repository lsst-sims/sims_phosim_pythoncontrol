#!/usr/bin/python
import os
import unittest
from AllChipsScriptGenerator import *
from optparse import OptionParser

class MockAllChipsScriptGenerator(AllChipsScriptGenerator):
  def _readTrimfileAndCalculateParams(self, rx, ry, sx, sy, ex):
    self.myrx = rx
    self.myry = ry
    self.mysx = sx
    self.mysy = sy
    self.myex = ex

    self.obshistid = '123456'
    self.filt = '2'
    return

  def _makePaths(self):
    return


class MockAllChipsScriptGenerator_Pbs(AllChipsScriptGenerator_Pbs):
  def _readTrimfileAndCalculateParams(self, rx, ry, sx, sy, ex):
    self.myrx = rx
    self.myry = ry
    self.mysx = sx
    self.mysy = sy
    self.myex = ex

    self.obshistid = '123456'
    self.filt = '2'
    return

  def _makePaths(self):
    return


class TestAllChipsScriptGenerator(unittest.TestCase):

  def _SetupWorkstation(self):
    self.imsimConfigFile = 'imsimConfig_workstation.cfg'
    self.extraidFile = 'clouds'
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
    s = MockAllChipsScriptGenerator('mockTrimFile', self.policy, self.extraidFile,
                                    self.rx, self.ry, self.sx, self.sy, self.ex)

    # This variable will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    self.assertEquals(s.trackingParFile, 'tracking_1234560.pars')

  def _SetupPbs(self):
    self._SetupWorkstation()
    self.imsimConfigFile = 'imsimConfig_minerva.cfg'
    self.scheduler = 'pbs'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitPbs(self):
    self._SetupPbs()
    # This will raise an exception if it does not read in the config file
    s = MockAllChipsScriptGenerator_Pbs('mockTrimFile', self.policy, self.extraidFile,
                                    self.rx, self.ry, self.sx, self.sy, self.ex)

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
