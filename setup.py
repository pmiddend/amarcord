import setuptools

setuptools.setup(
    name="amarcord",
    author="Philipp Middendorf <philipp.middendorf@desy.de>",
    author_email="sc@cfel.de",
    description="AMARCORD main application",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "amarcord-daemon = amarcord.cli.daemon:main",
            "amarcord-db-cli = amarcord.cli.admin:main",
            "amarcord-database-fuzzer = amarcord.cli.database_fuzzer:main",
            "amarcord-xfel-filesystem-ingester = amarcord.cli.xfel_filesystem_ingester:main",
            "amarcord-karabo-online-ingester = amarcord.cli.karabo_online_ingester:main",
        ]
    },
    # see https://github.com/dolfinus/setuptools-git-versioning
    setup_requires=["setuptools-git-versioning"],
    version_config=True,
    install_requires=[
        # for general DB access
        "SQLAlchemy==1.3.*",
        # for mysql
        "pymysql==1.0.*",
        # for caching_sha2_password access
        "cryptography==3.4.*",
        # for migrating a DB
        "alembic==1.5.*",
        # for attributi: parsing/printing a delta
        "isodate==0.6.*",
        # for verifying passwords in the DB
        "bcrypt==3.2.*",
        # to pretty-print attributi (yeah, it's a bit overkill)
        "lark-parser==0.11.*",
        # For the config file
        "PyYAML==5.4.*",
        # For the config file
        # (for accessing XDG_CONFIG_HOME)
        "xdg==5.0.*",
    ],
    extras_require={
        "daemon": [
            "pyzmq==22.0.*",
            "karabo-bridge==0.6.*",
            "numpy==1.19.*",
            "msgpack==1.0.*",
            "msgpack-types==0.1.*",
            "extra_data==1.5.*",
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
        ],
        "webserver": ["Flask==1.1.*", "Flask-Cors==3.0.*", "gunicorn==20.1.*"],
    },
    python_requires=">=3.8",
)
