#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

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

To Do:   Add pex_logging and pex_exceptions
         Remove all directory dependence - use environment variables

"""
from __future__ import with_statement
import os, re, sys
import math
import shutil
import subprocess
import string
import gzip

#import lsst.pex.policy as pexPolicy
#import lsst.pex.logging as pexLog
#import lsst.pex.exceptions as pexExcept

def makeChipImage(obshistid, filter, rx, ry, sx, sy, ex, datadir):

    """
    Create the chip image.
    """

    id  = 'R'+rx+ry+'_S'+sx+sy+'_E00'+ex
    cid = 'R'+rx+ry+'_S'+sx+sy
    
    raytraceCmdFile   = 'raytrace_%s_%s.pars' %(obshistid, id)
    #trimCatFile       = 'trimcatalog_%s_%s.pars' %(obshistid, cid)
    trimCatFile       = 'trimcatalog_%s_%s.pars' %(obshistid, id)
    raytraceParFile   = 'raytracecommands_%s_%s.pars' %(obshistid, id)
    backgroundParFile = 'background_%s_%s.pars' %(obshistid, id) 
    cosmicParFile     = 'cosmic_%s_%s.pars' %(obshistid, id)  
    outputFile        = 'output_%s_%s.fits' %(obshistid, id)

    # RUN THE RAYTRACE
    cmd = 'cat %s %s > %s' %(raytraceParFile, trimCatFile, raytraceCmdFile )
    subprocess.check_call(cmd, shell=True)
    os.chdir('raytrace/')
    cmd = './lsst < ../%s' %(raytraceCmdFile)
    subprocess.check_call(cmd, shell=True)
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
    shutil.move('raytrace/%s' %(image), 'ancillary/Add_Background/fits_files/%s' %(image))
    os.chdir('ancillary/Add_Background')
    cmd = './add_background < ../../%s' %(backgroundParFile)
    subprocess.check_call(cmd, shell=True)
    if os.access('fits_files/%s_settings' %(image), os.F_OK):
        os.remove('fits_files/%s_settings' %(image))

    # ADD COSMIC RAYS
    os.chdir('../../ancillary/cosmic_rays')
    cmd = ('./create_rays < ../../%s' %(cosmicParFile))
    subprocess.check_call(cmd, shell=True)
    os.remove('../Add_Background/fits_files/%s' %(image))

    f_in = open(outputFile, 'rb')
    f_out = gzip.open('%s.gz' %(outputFile), 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

    os.chdir('../..')

    axList = ['_C0', '_C1']
    ayList = ['0', '1', '2', '3', '4', '5', '6', '7']
    for ax in axList:
        for ay in ayList:
            os.chdir('ancillary/e2adc')

            eadc = 'e2adc_%s_%s%s%s_E00%s.pars' %(obshistid, cid, ax, ay, ex)
            imsim = 'imsim_%s_%s%s%s_E00%s.fits' %(obshistid, cid, ax, ay, ex)
            imsimFilter = 'imsim_%s_f%s_%s%s%s_E00%s.fits.gz' %(obshistid, filter, cid, ax, ay, ex)

            # RUN E2ADC CONVERTER
            print 'Running ./e2adc < ../../%s' %(eadc)
            cmd = './e2adc < ../../%s' %(eadc)
            subprocess.check_call(cmd, shell=True)

            shutil.move(imsim, '../../')
            os.chdir('../..')

            f_in = open(imsim, 'rb')
            f_out = gzip.open('%s.gz' %(imsim), 'wb')
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()

            shutil.move('%s.gz' %(imsim), '%s/%s' %(datadir, imsimFilter))

##     # CLEAN UP
##     axList = ['_C0', '_C1']
##     ayList = ['0', '1', '2', '3', '4', '5', '6', '7']
##     for ax in axList:
##         for ay in ayList:
##             os.remove('%s' %(eadc))

##     os.remove('%s' %(trimCatFile))
    os.remove('ancillary/cosmic_rays/%s' %(outputFile))            
    os.remove('ancillary/cosmic_rays/%s.gz' %(outputFile))
##     os.remove('chip_%s_%s.pars' %(obshistid, id))
##     os.remove('%s' %(cosmicParFile))
##     os.remove('%s' %(backgroundParFile))
##     os.remove('%s' %(raytraceCmdFile))
##     os.remove('%s' %(raytraceParFile))
    
    return

if __name__ == "__main__":
    
    if not len(sys.argv) == 9:
        print "usage: python chip.py obshistid filterNo rx ry sx sy ex datadir"
        quit()

    obshistid = sys.argv[1]
    filter = sys.argv[2]
    rx = sys.argv[3]
    ry = sys.argv[4]
    sx = sys.argv[5]
    sy = sys.argv[6]
    ex = sys.argv[7]
    datadir = sys.argv[8] 

    makeChipImage(obshistid, filter, rx, ry, sx, sy, ex, datadir)
