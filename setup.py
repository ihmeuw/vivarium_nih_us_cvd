#!/usr/bin/env python
import os
import sys

from setuptools import find_packages, setup

min_version, max_version = ("3.9", "3.11")

min_version_parts = tuple(int(v) for v in min_version.split("."))
max_version_parts = tuple(int(v) for v in max_version.split("."))
if not (min_version_parts <= sys.version_info[:2] <= max_version_parts):
    # Python 3.5 does not support f-strings
    py_version = ".".join([str(v) for v in sys.version_info[:3]])
    error = (
        "\n----------------------------------------\n"
        "Error: This repo requires python {min_version}-{max_version}.\n"
        "You are running python {py_version}".format(
            min_version=min_version, max_version=max_version, py_version=py_version
        )
    )
    print(error, file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    about = {}
    with open(os.path.join(src_dir, "vivarium_nih_us_cvd", "__about__.py")) as f:
        exec(f.read(), about)

    with open(os.path.join(base_dir, "README.rst")) as f:
        long_description = f.read()

    install_requirements = [
        "gbd_mapping>=3.1.0, <4.0.0",
        "vivarium>=2.1.1",
        "vivarium_public_health>=2.0.0",
        "click",
        "jinja2",
        "loguru",
        "numpy",
        "pandas",
        "pyyaml",
        "scipy",
        "tables",
    ]

    setup_requires = ["setuptools_scm"]

    data_requirements = ["vivarium_inputs[data]==4.1.0"]
    cluster_requirements = ["vivarium_cluster_tools>=1.3.13"]
    test_requirements = ["pytest"]

    setup(
        name=about["__title__"],
        description=about["__summary__"],
        long_description=long_description,
        license=about["__license__"],
        url=about["__uri__"],
        author=about["__author__"],
        author_email=about["__email__"],
        package_dir={"": "src"},
        packages=find_packages(where="src"),
        include_package_data=True,
        install_requires=install_requirements,
        extras_require={
            "test": test_requirements,
            "cluster": cluster_requirements,
            "data": data_requirements + cluster_requirements,
            "dev": test_requirements + cluster_requirements,
        },
        zip_safe=False,
        use_scm_version={
            "write_to": "src/vivarium_nih_us_cvd/_version.py",
            "write_to_template": '__version__ = "{version}"\n',
            "tag_regex": r"^(?P<prefix>v)?(?P<version>[^\+]+)(?P<suffix>.*)?$",
        },
        setup_requires=setup_requires,
        entry_points="""
            [console_scripts]
            make_artifacts=vivarium_nih_us_cvd.tools.cli:make_artifacts
            make_results=vivarium_nih_us_cvd.tools.cli:make_results
        """,
    )
