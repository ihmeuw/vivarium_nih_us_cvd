===============================
vivarium_nih_us_cvd
===============================

.. image:: https://zenodo.org/badge/508851666.svg
  :target: https://zenodo.org/doi/10.5281/zenodo.10671674

Cite as: 
Rajan Mudambi, Steve Bachmeier, Hussain Jafari, Kjell Swedin, Matthew Kappel, Sylvia Lutze, Alison Bowman, Caroline Kinuthia, & Abraham Flaxman. (2024). ihmeuw/vivarium_nih_us_cvd: Archival release (v1.0). Zenodo. https://zenodo.org/doi/10.5281/zenodo.10671674

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

  :~$ conda create --name=vivarium_nih_us_cvd python=3.12
  ...conda will download python and base dependencies...
  :~$ conda activate vivarium_nih_us_cvd
  (vivarium_nih_us_cvd) :~$ git clone https://github.com/ihmeuw/vivarium_nih_us_cvd.git
  ...git will copy the repository from github and place it in your current directory...
  (vivarium_nih_us_cvd) :~$ cd vivarium_nih_us_cvd
  (vivarium_nih_us_cvd) :~$ pip install -e .
  ...pip will install vivarium and other requirements...

Supported Python versions: 3.9, 3.10, 3.11, 3.12

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

You can run your simulation from the command line. 
With your conda environment active, you can run with, e.g.::

   (vivarium_nih_us_cvd) :~$ simulate run -vvv /<REPO_INSTALLATION_DIRECTORY>/vivarium_nih_us_cvd/src/vivarium_nih_us_cvd/model_specifications/nih_us_cvd.yaml -o /FILE/PATH/TO/SAVE/RESULTS -i src/vivarium_nih_us_cvd/artifacts/<STATE_NAME>.hdf

The simulation will run in one location at a time, enter the state you wish to 
run the simulation for in your call. The state name should be in lower case with 
an underscore between words. For example 'alabama', 'new_jersey' and 'district_of_columbia'.  

The ``-vvv`` flag will log verbosely, so you will get log messages every time
step. For more ways to run simulations, see the tutorials at
https://vivarium.readthedocs.io/en/latest/tutorials/running_a_simulation/index.html
and https://vivarium.readthedocs.io/en/latest/tutorials/exploration.html


Vivarium 3.x Upgrade (April 2026)
----------------------------------

This project was originally built against vivarium 2.x and
vivarium_public_health 2.2.0. In April 2026 the code was updated to run
on vivarium >= 3.0.0 and vivarium_public_health >= 4.0.0. Below is a
comprehensive summary of every change and why it was needed.

Dependency changes (``setup.py``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``vivarium`` pinned from ``>=2.1.1`` to ``>=3.0.0``
- ``vivarium_public_health`` pinned from ``==2.2.0`` to ``>=4.0.0``
- ``gbd_mapping`` removed from install requirements (it is only needed
  for artifact building on the IHME cluster, not for running simulations)
- ``vivarium_inputs[data]`` pin relaxed from ``==4.1.0`` to ``>=4.1.0``
- Python 3.12 added to ``python_versions.json``

``ConfigTree`` renamed to ``LayeredConfigTree``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vivarium 3.x extracted the configuration tree into a standalone package
called ``layered_config_tree``. All imports of ``ConfigTree`` from
``vivarium`` were replaced:

- ``src/vivarium_nih_us_cvd/components/causes/causes.py`` — six
  occurrences: class attribute type hints and method signatures
- ``src/vivarium_nih_us_cvd/plugins/causes_parser.py`` — ten
  occurrences: method signatures and docstrings

``pkg_resources`` replaced with ``importlib.resources``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``pkg_resources`` (from ``setuptools``) was deprecated and removed in
modern Python/setuptools versions. The single usage in
``src/vivarium_nih_us_cvd/plugins/causes_parser.py`` was replaced:

- ``from pkg_resources import resource_filename`` →
  ``from importlib.resources import files``
- ``resource_filename(package, config_file)`` →
  ``str(files(package).joinpath(config_file))``

``importlib.resources`` is part of the Python standard library (3.9+)
and requires no additional dependencies.

Population configuration keys renamed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vivarium 3.x renamed the population configuration keys. In both
``nih_us_cvd.yaml`` and ``paf_calculation.yaml``:

- ``age_start`` → ``initialization_age_min``
- ``age_end`` → ``initialization_age_max``
- ``exit_age`` → ``untracking_age``

Results module path change
~~~~~~~~~~~~~~~~~~~~~~~~~~

``vivarium_public_health`` moved its results/stratification module:

- ``vivarium_public_health.metrics.stratification`` →
  ``vivarium_public_health.results.stratification``

This affected the ``ResultsStratifier`` import in
``src/vivarium_nih_us_cvd/components/observers.py``.

``register_observation`` renamed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vivarium 3.x renamed the results registration method:

- ``builder.results.register_observation(...)`` →
  ``builder.results.register_adding_observation(...)``

Nine call sites in ``observers.py`` were updated (``ContinuousRiskObserver``,
``HealthcareVisitObserver``, ``CategoricalColumnObserver``,
``LifestyleObserver``, ``BinnedRiskObserver``, ``JointPAFObserver``).

Model spec YAML: ``metrics`` → ``results``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The component section key that registers metric observers changed from
``metrics`` to ``results`` in the vivarium_public_health component list
within ``nih_us_cvd.yaml``.

Risk factor data sources in YAML
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vivarium 3.x requires risk factors with constant exposure values
(outreach, polypill, lifestyle) to declare their data sources and
distribution type under ``configuration.risk_factor.<name>`` using the
new ``data_sources`` format. The following was added to
``nih_us_cvd.yaml``::

    risk_factor.outreach:
        data_sources:
            exposure: 0
    risk_factor.polypill:
        data_sources:
            exposure: 0
    risk_factor.lifestyle:
        distribution_type: "dichotomous"
        data_sources:
            exposure: .0855

``RateTransition`` now requires ``transition_rate``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``RateTransition`` constructor in vivarium 3.x added a required
``transition_rate`` parameter. ``CompositeRateTransition`` in
``src/vivarium_nih_us_cvd/components/causes/transition.py`` was updated
to pass ``transition_rate=0.0`` in the ``super().__init__()`` call (the
actual rate is built later in ``setup``).

The explicit ``self.population_view = builder.population.get_view(["alive"])``
was also removed, since vivarium 3.x manages population views
automatically via ``columns_required`` on the ``Component`` base class.

``MediatedRiskEffect.build_all_lookup_tables`` override
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vivarium 3.x's ``RiskEffect`` now builds PAF lookup tables by default
during ``build_all_lookup_tables``. This project uses **joint PAFs** from
a separate PAF calculation simulation, so the per-risk PAF data does not
exist in the artifact.

``MediatedRiskEffect`` in ``src/vivarium_nih_us_cvd/components/effects.py``
was given a custom ``build_all_lookup_tables`` method that:

- Loads only the relative risk data (skipping PAF loading entirely)
- Handles the ``CategoricalSBPRisk`` edge case by inferring distribution
  type from the risk name when standard lookup fails

Additionally, the reference to ``self.target_modifier`` was updated to
``self.adjust_target`` to match the vivarium 3.x API rename.

NumPy dtype fix in ``Treatment``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``src/vivarium_nih_us_cvd/components/treatment.py`` had two calls to
``np.array(p_medication)`` where the DataFrame contained mixed types.
NumPy 2.x (required by the new dependency chain) raises an error when
creating arrays with ambiguous dtypes. Both calls were changed to
``np.array(p_medication, dtype=float)``.

``.gitignore`` additions
~~~~~~~~~~~~~~~~~~~~~~~~

- ``*.hdf`` — artifact files are large and tracked via git-lfs, not
  regular git
- ``sim_output/`` — simulation output directories

USA-Level Artifact
~~~~~~~~~~~~~~~~~~

A script ``build_usa_artifact.py`` was added to create a USA-level
artifact by population-weighted aggregation of the 51 state artifacts.
For each demographic cell (sex × age × year), state values are weighted
by ``state_population / total_population``, and the population structure
is summed. The resulting artifact lives at
``src/vivarium_nih_us_cvd/artifacts/united_states_of_america.hdf``.

See ``notebooks/01_explore_usa_artifact.ipynb`` for visual exploration
of the USA artifact, and ``notebooks/02_build_gbd_usa_artifact_and_compare.ipynb``
for documentation on building a USA artifact directly from GBD data and
comparing the two approaches.
