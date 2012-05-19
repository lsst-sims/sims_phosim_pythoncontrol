#!/usr/bin/python2.6
import os
import unittest
from Focalplane import *

class TestFocalplane(unittest.TestCase):

  def setUp(self):
    self.f = Focalplane('12345678','r')

  def test_Init(self):
    self.assertEqual(self.f.obsid, '12345678-fr')
    self.assertEqual(self.f.parsDictionary['track'], 'track_12345678.pars')
    return

if __name__ == '__main__':
    unittest.main()
