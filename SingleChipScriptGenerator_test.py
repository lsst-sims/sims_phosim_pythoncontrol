#!/usr/bin/python2.6
import os
import unittest
from SingleChipScriptGenerator import *
from optparse import OptionParser

class TestSingleChipScriptGenerator(unittest.TestCase):

  def _SetupWorkstation(self):
    self.baseDir = os.path.dirname(__file__)
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_workstation.cfg')
    self.scheduler = 'csh'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)
    self.obshistid = '123456'
    self.filter = 'r'
    self.filt = '2'
    self.centid = '0'
    self.centroidPath = '/tmp/centroidPath'
    self.stagePath2 = '/tmp/stagePath2'
    self.paramDir = 'paramDir'
    self.trackingParFile = 'trackingParFile'


  def test_InitWorkstation(self):
    """
    Tests the import and initialization of SingleChipScriptGenerator class.  Initialization
    includes parsing the .cfg file
    """
    self._SetupWorkstation()
    # This will raise an exception if it does not read in the config file
    s = SingleChipScriptGenerator(self.policy, self.obshistid, self.filter, self.filt,
                                  self.centid, self.centroidPath, self.stagePath2,
                                  self.paramDir, self.trackingParFile)

    # This variable will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise

  def _SetupPbs(self):
    self._SetupWorkstation()
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_minerva.cfg')
    self.scheduler = 'pbs'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitPbs(self):
    self._SetupPbs()
    # This will raise an exception if it does not read in the config file
    s = SingleChipScriptGenerator_Pbs(self.policy, self.obshistid, self.filter, self.filt,
                                      self.centid, self.centroidPath, self.stagePath2,
                                      self.paramDir, self.trackingParFile)

    # These variables will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    try:
      s.username
    except:
      raise

if __name__ == '__main__':
    unittest.main()
