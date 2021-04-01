import setuptools

setuptools.setup(
    name="amarcord",
    version="0.2pre6",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="AMARCORD main application",
    packages=setuptools.find_packages(),
    entry_points={"console_scripts": ["amarcord-daemon = amarcord.daemon:mymain"]},
    install_requires=[
        # for general DB access
        "SQLAlchemy==1.3.*",
        # for attributi: parsing a natural delta (not needed in general?)
        # "lark-parser==0.11.*",
        # for attributi: parsing/printing a delta
        "isodate==0.6.*",
        # for verifying passwords in the DB
        "bcrypt==3.2.*",
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
            "lark-parser==0.11.*",
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
