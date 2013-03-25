===============================================
Python ImSim Control Files
===============================================

This file contains instructions for ImSim/PhoSim v3.2.x and later.

**For running ImSim/PhoSim v-3.0.x and earlier, see README.v3.0.x.txt.**

The Python control files have been completely reworked and greatly
simplified since v3.0.x.

==========================
REQUIREMENTS
==========================
1. The proper revision of PhoSim.
2. The "fitsverify" executable from the package
   http://heasarc.gsfc.nasa.gov/docs/software/ftools/fitsverify/
   must be in your path for the raytracing stage (see "FILE VERIFICATION"
   below).  If this is problematic, its usage can be disabled
   (see "FILE VERIFICATION" below).
3. Python 2.5 or later


==========================
CONFIGURATION
==========================

These files are for use with the LSST Image Simulator ("ImSim" or
"PhoSim") v3.2.x and greater.  They do not need to be in the PhoSim
source tree when they are executed.  However, **you must be able
to import phosim.py (located in the phosim.git repository root).**
See step 3) below for how to do this.

The correct procedure is to:
  1) Check out a tagged revision of phosim.git
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

  3) Adjust your configuration so that you can import 'phosim.py' from
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
     You can also download Jeff's current copy from:
        http://www.phys.washington.edu/users/gardnerj/files/imsim/data_phosim_06112012.tar
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
     In order to build this directory, do *all* of the the following:
       a) Copy the 'data' directory from phosim.git repo to a shared
          location (where you will point to it with 'shared_data_path')
       b) Copy 'agnSED', 'flatSED', 'galaxySED', 'ssmSED', and
         'starSED' into the 'data/SEDs' directory.
       c) **Copy 'default_instcat' from the phosim.git repo into
          'shared_data_path'.**

  4) Edit the config file (see exampleConfig_workstation.cfg), which
     includes information specific to your Python and storage environment.

  5) Execute the Python control package from any directory
     (See "USAGE" below).

==========================
REVISIONS
==========================

The tags of the Python control package match the ImSim/PhoSim tag with
which they are designed to interface.


==========================
LOG OUTPUT
==========================

Python_controls uses the Python 'logging' module instead of writing
to stdout.  Logs are stored in 'log_dir'/<observation_id> where
'log_dir' is specified in the config file.  You can override this
behavior by using the commandline flag --logtostdout.  The names
of the log files in that directory will be as follows:
  fullFocalplane_<observation_id>.log,
  onechip_<observation_id>_<chip_id>_<exposure_id>.log
  onechip_<observation_id>_<chip_id>_<exposure_id>_stdout.log

Stdout from the raytracing portion of phosim (i.e. from phosim.py and
the 'raytrace' and 'e2adc' executables) is also redirected to a
separate log file (also in 'log_dir'/<observation_id>) if
'log_stdout' is set to 'true' in the config file.


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
You may use either relative or absolute paths for any of the arguments, e.g.:
  % /local/gardnerj/lsst/git/python_control/fullFocalplane.py \
  /local/gardnerj/lsst/trims/obsid99999999/metadata_99999999.dat \
  /local/gardnerj/lsst/git/python_control/exampleConfig_workstation.cfg \
  -c /local/gardnerj/lsst/git/python_control/clouds_nobackground

It does not matter which directory you execute fullFocalPlane.py
from.  It's execution environment is governed by the config file.

Execution will occur in 'scratch_exec_path' (set in the config fie).
The phosim subdirectories 'data', 'work', and 'output' will be created
here (in phosim.py these are stored in the variables 'datadir',
'workdir', and 'outputdir').

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

To raytrace a chip/exposure, simple execute the corresponding shell
script.  A simple way to execute the raytrace scripts in parallel on
<ncores> cores is:
  % cat execmanifest_raytrace_<observation_id>.txt | xargs -P <ncores> -n 1 csh
(See http://blog.labrat.info/20100429/using-xargs-to-do-parallel-processing/)

It does not matter which directory you execute the shell scripts from.
Their execution environment is governed by the config file.

If you take a peek inside the shell scripts, you will see that they
provide a thin wrapper around onechip.py.  For diagnostics and
testing, you may wish to run onechip.py by hand.  See that file for
more documentation or run 'onechip.py -h'.

Note the shell script will append $1 (the first command-line argument)
to the onechip.py command line.  To append multiple arguments, enclose
them in double-quotes, e.g.
  exec_raytrace_999999992_R22_S00_E000.csh "-e -k -l"

Output from the raytracing stage will be stored in the following
locations:
  'save_path': This will contain 'eimage' and 'raw' directories and
               their subdirs.
  'log_dir'/<observation_id>: logs.

IMPORTANT: When something goes wrong, try looking in the logs, as
           errors are logged there, too.


==========================
SIMPLE TEST RUN
==========================
Included in the source is a small test trimfile/catalog in
testdata/obsid99999999 that is designed to run only a few
minutes per chip.  To run this:
  1. cd to source directory
  2. Edit exampleConfig_workstation.cfg to set the paths to
     something that makes sense for your system and save it
     to MyTestConfig.cfg.
  3. run:

 fullFocalplane.py testdata/obsid99999999/metadata_99999999.dat MyTestConfig.cfg -c clouds_nobackground


==========================
FILE VERIFICATION
==========================

"verifyFiles.py" does not currently work with 3.2.x.  However, files
are verified automatically by fullFocalPlane.py and onechip.py.  The
former verifies preprocessing output after it has been copies to
<stage_path>.  The latter verifies raytrace output both in
the <scratch_exec_path>/output (it also does a fitsverify here)
and after the output has been copies to shared storage (<save_path>).

The fitsverify executable must be in your path for raytrace output
verification to work.  If this is a problem, you can optionally skip
the fitsverify step by invoking onechip.py with '--no_fitsverify'.

File verification is done via the classes in PhosimVerifier.py.
Eventually, Jeff will write an updated wrapper script for it.

==========================
MINIMIZING DATA TRANSFER
==========================

If you are running on a platform that has limited I/O bandwidth,
you may wish to calculate the atmosphere screen as part of the
raytrace step, rather than in preprocessing.  This increases
the runtime of the raytrace by only a minute or so, but with
the advantage that you save about 250MB in data transfer.

Do calculate atmosphere screens as part of the raytrace step, execute
fullFocalPlane with the --skip_atmoscreens (or -a) option.


===============================
SCHEDULER-SPECIFIC ENVIRONMENTS
===============================

As an example of implementing scheduler-specific versions, a skeleton
for 'pbs' has been provided.  In fullFocalplane.py:DoPreproc(), one
can see an example of how to tell PhosimManager.PhosimPreprocessor to
use an alternate ScriptWriter class, in this case one made for PBS.
The ScriptWriter instance is initialized in the PhosimPreprocessor
constructor.  After that, a pbs-specific method is called to read
in pbs-specific configuration paramaters from the config file.

See exampleConfig_pbs.cfg for an example PBS config file, and
ScriptWriter.py:PbsRaytraceScriptWriter for a skeleton script writer.


