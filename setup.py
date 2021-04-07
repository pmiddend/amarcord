import setuptools

setuptools.setup(
    name="amarcord",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="AMARCORD main application",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "amarcord-daemon = amarcord.daemon:mymain",
            "amarcord-admin = amarcord.cli.admin:main",
            "amarcord-database-fuzzer = amarcord.cli.database-fuzzer:main",
        ]
    },
    # see https://github.com/dolfinus/setuptools-git-versioning
    setup_requires=["setuptools-git-versioning"],
    version_config=True,
    install_requires=[
        # for general DB access
        "SQLAlchemy==1.3.*",
        # for migrating a DB
        "alembic==1.5.*",
        # for attributi: parsing/printing a delta
        "isodate==0.6.*",
        # for verifying passwords in the DB
        "bcrypt==3.2.*",
        # to pretty-print attributi (yeah, it's a bit overkill)
        "lark-parser==0.11.*",
    ],
    extras_require={
        "daemon": [
            "pyzmq==22.0.*",
            "karabo-bridge==0.6.*",
            "numpy==1.19.*",
        ],
        "gui": [
            "PyQt5==5.15.*",
            "PyQt5-stubs==5.15.*",
            "pyzmq==22.0.*",
            "PyYAML==5.4.*",
            # For the plotting stuff
            "pandas==1.2.*",
            # For validating pubchem cids
            "PubChemPy==1.0.*",
            # For printing "x second(s) ago" in the comments
            "humanize==3.2.*",
            # Not really directly used I think
            "numpy==1.19.*",
            # Also for the plots
            "matplotlib==3.3.*",
            # For validating units
            "pint==0.16.*",
            # For accessing XDG_CONFIG_HOME
            "xdg==5.0.*",
        ],
        "webserver": [
            "Flask==1.1.*",
            "Flask-Cors==3.0.*",
        ],
    },
    python_requires=">=3.8",
)
