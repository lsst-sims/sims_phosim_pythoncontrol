This file contains instructions for ImSim/PhoSim v3.2.x and later.

**For running ImSim/PhoSim v-3.0.x and earlier, see README.v3.0.x.txt.**

==========================
Python ImSim Control Files
==========================

The Python control files have been completely reworked and greatly
simplified since v3.0.x.

These files are for use with the LSST Image Simulator ("ImSim" or
"PhoSim") v3.2.x and greater.  They do not need to be in the PhoSim
source tree when they are executed.  However, **you must be able
to import phosim.py in the phosim.git repository root.** See step
3) below for how to do this.

The correct procedure is to:
  1) Check out proper revision of phosim.git
  2) Check out a compatible revision of the Python control package
     (The compatable version of the python control package will share
      the same tag with PhoSim: e.g.
       % git clone git@git.lsstcorp.org:LSST/sims/phosim.git
       % cd phosim
       % git checkout refs/tags/v3.2.3
       % cd ../
       % git clone git@git.lsstcorg.org:LSST/sims/python_control.git
       % cd python_control
       % git checkout refs/tags/v3.2.3

  3) Adjust your configuration so that you can import python.py from
     the phosim.git repository.  This can be done a few different
     ways:
       a) (recommended) Configure your PYTHONPATH environment to point
          to the phosim.git repo directory.  For example, on csh do:
              setenv PYTHONPATH /path/to/phosim:{$PYTHONPATH}
       b) Make a symbolic link from the python_controls directory to
          phosim.py (This will work because phosim.py has no other dependencies):
              ln -s /path/to/phosim/phosim.py /path/to/python_control/phosim.py
       c) Just copy phosim.py into the python_control directory.  This
          will work because phosim.py has no other dependencies.

  4) Build a 'data' directory for holding SED and instrument data.
     Phosim v3.2.x and greater expects the "data" directory to be
     organized in a specific way.  This will be 'shared_data_path'
     in your config file:
       <shared_data_path>/
            SEDs/
                agnSED/
	        flatSED/
	        galaxySED/
	        ssmSED/
	        starSED/
            atmosphere/
            aux/
            cosmic_rays/
 	    sky/
	    [instrument: e.g. "lsst", "subaru"]/
     In order to build this directory, do all of the the following:
       a) Copy the 'data' directory from phosim.git repo to a shared
          location (where you will point to it with 'shared_data_path')
       b) Copy 'agnSED', 'flatSED', 'galaxySED', 'ssmSED', and
         'starSED' into the 'data/SEDs' directory.
       c) **Copy 'default_instcat' from the phosim.git repo into
          'shared_data_path'.**

  4) Edit the config file (see exampleConfig_workstation.cfg), which
     includes information specific to your Python and storage environment.

  5) Execute the Python control package from any directory.

==========================
REQUIREMENTS
==========================
1. The proper revision of PhoSim.
2. The "fitsverify" executable from the package
   http://heasarc.gsfc.nasa.gov/docs/software/ftools/fitsverify/
   must be in your path for the raytracing stage (see below).
3. Python 2.5 or later


==========================
REVISIONS
==========================

The tags of the Python control package match the ImSim/PhoSim tag with
which they are designed to interface.


==========================
USAGE
==========================

These scripts divide execution of the ImSim/PhoSim workflow into two distict
stages: "preprocessing" and "raytrace".

Preprocessing:
--------------
Run 'fullFocalplane.py' to perform the preprocessing step and to
generate shell scripts for running the raytracing stage for each chip
(one shell script per chip).  Run 'fullFocalplane.py -h' for usage.
You may use absolute paths for any of the arguments, e.g.:
  % /local/gardnerj/lsst/git/python_control/fullFocalplane.py \
  /local/gardnerj/lsst/trims/obsid99999999/metadata_99999999.dat \
  /local/gardnerj/lsst/git/python_control/exampleConfig_workstation.cfg \
  -c /local/gardnerj/lsst/git/python_control/clouds_nobackground

It does not matter which directory you execute fullFocalPlane.py
from.  It's execution environment is governed by the config file.

Execution will occur in 'scratch_exec_path'.  The subdirectories
'data', 'work', and 'output' will be created here.

Output from this stage will be stored in the following locations:
  'stage_path'/<observation_id>: All data files required for raytrace
                                 step.
  'log_dir'/<observation_id>: Log output.

IMPORTANT: When something goes wrong, try looking in the logs, as
           errors are logged there, too.

Raytracing:
-----------
Each raytrace shell script is stored in 'stage_path'/<observation_id>
and a manifest of the shell scripts is listed in the file
  'stage_path'/<observation_id>/execmanifest_raytrace_<observation_id>.txt

A simple way to execute the raytrace scripts in parallel on <ncores>
cores is:
  % cat execmanifest_raytrace_<observation_id>.txt | xargs -P <ncores> -n 1 csh
(See http://blog.labrat.info/20100429/using-xargs-to-do-parallel-processing/)

It does not matter which directory you execute the shell scripts from.
Their execution environment is governed by the config file.

If you take a peek inside the shells scripts. You will see that they
provide a thin wrapper around onechip.py.  For diagnostics and
testing, you may wish to run onechip.py by hand.  See that file for
more documentation or run 'onechip.py -h'.

Output from the raytracing stage will be stored in the following
locations:
  'save_path': This will contain 'eimage' and 'raw' directories and
               their subdirs.
  'log_dir'/<observation_id>: logs.

IMPORTANT: When something goes wrong, try looking in the logs, as
           errors are logged there, too.


==========================
VERIFYING FILES
==========================

"verifyFiles.py" can be used to verify the output of both the
preprocessing and raytrace stages.  The control scripts automatically
verify proprocessing output (in shared storage) as well as raytracing
output (on the execution node).  The details for parsing the output
from each of these steps is given above.

The control scripts do not automatically verify that an entire focalplane
has completed the raytracing stage and been stored in <savePath>.  To
do this, you can execute:
  verifyFiles.py <obshistid> <filter> <savePath>

Note that <savePath> also has to include "imSim/PT1.2" if it is
present (i.e. it should be the path to the "eimage" and "raw"
directories).

============================
SCHEDULER-SPECIFIC EXECUTION
============================

As an example of implementing scheduler-specific versions, a skeleton
for 'pbs' has been provided.  In fullFocalplane.py:DoPreproc(), one
can see an example of how to tell PhosimManager.PhosimPreprocessor to
use an alternate ScriptWriter class, in this case one made for PBS.
The ScriptWriter instance is initialized in the PhosimPreprocessor
constructor.  After that, a pbs-specific method is called to read
in pbs-specific configuration paramaters from the config file.

See exampleConfig_pbs.cfg for an example PBS config file, and
ScriptWriter.py:PbsRaytraceScriptWriter for a skeleton script writer.


