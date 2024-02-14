===============================
vivarium_nih_us_cvd
===============================

Research repository for the vivarium_nih_us_cvd project.

.. contents::
   :depth: 1

Installation
------------

You will need ``git``, ``git-lfs`` and ``conda`` to get this repository
and install all of its requirements.  You should follow the instructions for
your operating system at the following places:

- `git <https://git-scm.com/downloads>`_
- `git-lfs <https://git-lfs.github.com/>`_
- `conda <https://docs.conda.io/en/latest/miniconda.html>`_

Once you have all three installed, you should open up your normal shell
(if you're on linux or OSX) or the ``git bash`` shell if you're on windows.
You'll then make an environment, clone this repository, then install
all necessary requirements as follows::

  :~$ conda create --name=vivarium_nih_us_cvd python=3.11
  ...conda will download python and base dependencies...
  :~$ conda activate vivarium_nih_us_cvd
  (vivarium_nih_us_cvd) :~$ git clone https://github.com/ihmeuw/vivarium_nih_us_cvd.git
  ...git will copy the repository from github and place it in your current directory...
  (vivarium_nih_us_cvd) :~$ cd vivarium_nih_us_cvd
  (vivarium_nih_us_cvd) :~$ pip install -e .
  ...pip will install vivarium and other requirements...

Supported Python versions: 3.9, 3.10, 3.11

Note the ``-e`` flag that follows pip install. This will install the python
package in-place, which is important for making the model specifications later.

Cloning the repository should take a fair bit of time as git must fetch
the data artifact associated with the demo (several GB of data) from the
large file system storage (``git-lfs``). **If your clone works quickly,
you are likely only retrieving the checksum file that github holds onto,
and your simulations will fail.** If you are only retrieving checksum
files you can explicitly pull the data by executing ``git-lfs pull``.

Vivarium uses the Hierarchical Data Format (HDF) as the backing storage
for the data artifacts that supply data to the simulation. You may not have
the needed libraries on your system to interact with these files, and this is
not something that can be specified and installed with the rest of the package's
dependencies via ``pip``. If you encounter HDF5-related errors, you should
install hdf tooling from within your environment like so::

  (vivarium_nih_us_cvd) :~$ conda install hdf5

The ``(vivarium_nih_us_cvd)`` that precedes your shell prompt will probably show
up by default, though it may not.  It's just a visual reminder that you
are installing and running things in an isolated programming environment
so it doesn't conflict with other source code and libraries on your
system.


Usage
-----

You'll find six directories inside the main
``src/vivarium_nih_us_cvd`` package directory:

- ``artifacts``

  This directory contains all input data used to run the simulations.
  You can open these files and examine the input data using the vivarium
  artifact tools.  A tutorial can be found at https://vivarium.readthedocs.io/en/latest/tutorials/artifact.html#reading-data

- ``components``

  This directory is for Python modules containing custom components for
  the vivarium_nih_us_cvd project. You should work with the
  engineering staff to help scope out what you need and get them built.

- ``data``

  If you have **small scale** external data for use in your sim or in your
  results processing, it can live here. This is almost certainly not the right
  place for data, so make sure there's not a better place to put it first.

- ``model_specifications``

  This directory should hold all model specifications and branch files
  associated with the project.

- ``results_processing``

  Any post-processing and analysis code or notebooks you write should be
  stored in this directory.

- ``tools``

  This directory hold Python files used to run scripts used to prepare input
  data or process outputs.


Running Simulations
-------------------

First, you will need to update the model specifications file to be the location you wish to run. 
There are options for 3 example states: California, Illinois and Florida. The default location is California. 
To update the location, go to the cloned repository. Then open the src folder, then vivarium_nih_us_cvd, 
model_specifications, and open the nih_us_cvd.yaml file. On line 94, change the file 
path '../artifacts/california.hdf' to be your location of choice. For example, 
'../artifacts/illinois.hdf' or '../artifacts/florida.hdf'. 

If you need other location data for a different state, please reach out to the 
owner of this repository. 

Once this is updated, you can run your simulation from the command line. 
With your conda environment active, you can run with, e.g.::

   (vivarium_nih_us_cvd) :~$ simulate run -vvv /<REPO_INSTALLATION_DIRECTORY>/vivarium_nih_us_cvd/src/vivarium_nih_us_cvd/model_specifications/<MODEL_SPEC_FILE_NAME>.yaml -o /FILE/PATH/TO/SAVE/RESULTS 

The ``-vvv`` flag will log verbosely, so you will get log messages every time
step. For more ways to run simulations, see the tutorials at
https://vivarium.readthedocs.io/en/latest/tutorials/running_a_simulation/index.html
and https://vivarium.readthedocs.io/en/latest/tutorials/exploration.html