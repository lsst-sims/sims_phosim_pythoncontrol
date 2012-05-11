#!/usr/bin/python2.6

"""

Brief:   Python script to create a single chip image.

Date:    March 07, 2011
Author:  Nicole Silvestri, U. Washington, nms@astro.washington.edu
Updated: March 29, 2011

Usage:   python chip.py [options]
Options: obshistid: obshistid of the sensor observation
         filter: numerical filter designation (u=0,g=1,r=2,i=3,z=4,y=5)
         rx: raft x value (0, 1, 2, 3, 4)
         ry: raft y value (0, 1, 2, 3, 4)
         sx: sensor x value (0, 1, 2)
         sy: sensor y value (0, 1, 2)
         ex: exposure (snap) value (0, 1)
         datadir: save directory location for the resulting images  

Notes:   On Cluster, must have LSST stack with afw setup.  Can also
         use the stack version of Python.

         To run standalone, you must have the following files in your
         working directory, where id=R+rx+ry_S+sx+sy_E00+ex:
         
         trimcatalog_%s_%s.pars' %(obshistid, cid)
         raytracecommands_%s_%s.pars' %(obshistid, id)
         background_%s_%s.pars' %(obshistid, id) 
         cosmic_%s_%s.pars' %(obshistid, id)
         tracking_%s.pars %(obshistid)
         cloudscreen_%s_%s.fits %(obshistid, screen)
         atmospherescreen_%s_%s_*.fits %(obshistid, screen)
         e2adc_%s_%s.pars %(obshistid, screen)

Notation: For naming the rafts, sensors, amplifiers, and exposures, we
          obey the following convention:
             cid:    Chip/sensor ID string of the form 'R[0-4][0-4]_S[0-2][0-2]'
             ampid:  Amplifier ID string of the form 'cid_C[0-1][0-7]'
             expid:  Exposure ID string of the form 'E[0-9][0-9][0-9]'
             id:     Full Exposure ID string of the form 'cid_expid'
             obshistid: ID of the observation from the trim file with the 'extraID'
                        digit appended ('clouds'=0, 'noclouds'=1).
"""
from __future__ import with_statement
import os, re, sys
import math
import shutil
import subprocess
import string
import gzip
import time

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


def readAmpList(filename, cid):
    ampList = []
    with open(filename, 'r') as ampFile:
        for line in ampFile.readlines():
            if line.startswith('%s_' %cid):
                ampList.append(line.split()[0])
    return ampList

def makeChipImage(obshistid, filter, cid, expid, datadir):

    """
    Create the chip image.
    """

    id = '%s_%s' %(cid, expid)
    
    lsstCmdFile       = 'lsst_%s_%s.pars' %(obshistid, id)
    trimCatFile       = 'trimcatalog_%s_%s.pars.gz' %(obshistid, id)
    raytraceCmdFile   = 'raytracecommands_%s_%s.pars' %(obshistid, id)
    backgroundParFile = 'background_%s_%s.pars' %(obshistid, id) 
    cosmicParFile     = 'cosmic_%s_%s.pars' %(obshistid, id)  
    outputFile        = 'output_%s_%s.fits' %(obshistid, id)

    # RUN THE RAYTRACE
    trimCatTmp = '_tmp_%s' %trimCatFile
    if os.path.isfile(trimCatTmp):
      os.remove(trimCatTmp)
    cmd = 'gunzip -c %s > %s' %(trimCatFile, trimCatTmp)
    subprocess.check_call(cmd, shell=True)
    cmd = 'cat %s %s > %s' %(raytraceCmdFile, trimCatTmp, lsstCmdFile )
    subprocess.check_call(cmd, shell=True)
    os.remove(trimCatTmp)
    os.chdir('raytrace/')
    cmd = 'time ./lsst < ../%s' %(lsstCmdFile)
    sys.stderr.write('Running: %s\n' %cmd)
    with WithTimer() as t:
      subprocess.check_call(cmd, shell=True)
    t.PrintWall('lsst', sys.stderr)
    os.chdir('..')

    eimage = 'eimage_%s_f%s_%s.fits' %(obshistid, filter, id)
    image = 'imsim_%s_%s.fits' %(obshistid, id)
    shutil.copyfile('raytrace/%s' %(image), '%s/%s' %(datadir, eimage))

    # Using python's gzip module...
    f_in = open('%s/%s' %(datadir, eimage), 'rb')
    f_out = gzip.open('%s/%s.gz'%(datadir, eimage), 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

##    This seems easier!!
##    cmd = 'gzip -f %s/%s' %(datadir, eimage)
##    subprocess.check_call(cmd, shell=True)

    # ADD BACKGROUND
    if not os.path.isdir('ancillary/Add_Background/fits_files'):
        os.mkdir('ancillary/Add_Background/fits_files')
    shutil.move('raytrace/%s' %(image), 'ancillary/Add_Background/fits_files/%s' %(image))
    os.chdir('ancillary/Add_Background')
    cmd = 'time ./add_background < ../../%s' %(backgroundParFile)
    sys.stderr.write('Running: %s\n' %cmd)
    with WithTimer() as t:
        subprocess.check_call(cmd, shell=True)
    t.PrintWall('add_background', sys.stderr)
    if os.access('fits_files/%s_settings' %(image), os.F_OK):
        os.remove('fits_files/%s_settings' %(image))

    # ADD COSMIC RAYS
    os.chdir('../../ancillary/cosmic_rays')
    cmd = 'time ./create_rays < ../../%s' %(cosmicParFile)
    sys.stderr.write('Running: %s\n' %cmd)
    with WithTimer() as t:
        subprocess.check_call(cmd, shell=True)
    t.PrintWall('create_rays', sys.stderr)
    os.remove('../Add_Background/fits_files/%s' %(image))

    f_in = open(outputFile, 'rb')
    f_out = gzip.open('%s.gz' %(outputFile), 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    os.chdir('../..')


    # RUN E2ADC CONVERTER
    ampList = readAmpList('lsst/segmentation.txt', cid)
    os.chdir('ancillary/e2adc')
    eadc = 'e2adc_%s_%s.pars' %(obshistid, id)
    cmd = 'time ./e2adc < ../../%s' %(eadc)
    sys.stderr.write('Running: %s\n' %cmd)
    with WithTimer() as t:
        subprocess.check_call(cmd, shell=True)
    t.PrintWall('e2adc', sys.stderr)

    print 'From %s:' %os.getcwd()
    for ampid in ampList:
        imsim = 'imsim_%s_%s_%s.fits' %(obshistid, ampid, expid)
        imsimFilter = 'imsim_%s_f%s_%s_%s.fits.gz' %(obshistid, filter, ampid, expid)
        cmd = 'gzip %s' % imsim
        subprocess.check_call(cmd, shell=True)
        imsim += '.gz'
        target = os.path.join('../..', datadir, imsimFilter)
        print '-- Moving', imsim, 'to', target
        shutil.move(imsim, target)
    os.chdir('../..')
      
##     os.remove('%s' %(trimCatFile))
    os.remove('ancillary/cosmic_rays/%s' %(outputFile))            
    os.remove('ancillary/cosmic_rays/%s.gz' %(outputFile))
##     os.remove('chip_%s_%s.pars' %(obshistid, id))
##     os.remove('%s' %(cosmicParFile))
##     os.remove('%s' %(backgroundParFile))
##     os.remove('%s' %(lsstCmdFile))
##     os.remove('%s' %(raytraceCmdFile))

    print 'chip.py complete.'
    
    return

if __name__ == "__main__":
    
    if not len(sys.argv) == 6:
        print "usage: python chip.py obshistid filterNo cid expid datadir"
        quit()

    obshistid = sys.argv[1]
    filter = sys.argv[2]
    cid = sys.argv[3]
    expid = sys.argv[4]
    datadir = sys.argv[5]

    with WithTimer() as t:
        makeChipImage(obshistid, filter, cid, expid, datadir)
    t.PrintWall('chip.py', sys.stderr)
