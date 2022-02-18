let
  # pkgs = import (builtins.fetchTarball {
  #     name = "nixpkgs-unstable-2021-11-24";
  #     url = "https://github.com/nixos/nixpkgs/archive/40c3bc3b0876f82b7ebf6816ab32a3c0c6e91133.tar.gz";
  #     sha256 = "15w604s1yjvyxg2pd76d7y67242ifq2dvprmh9ympbapfyxx8qdz";
  #   }) {};
  pkgs = import (builtins.fetchTarball {
      name = "nixpkgs-staging-2021-11-24";
      url = "https://github.com/nixos/nixpkgs/archive/97cf632e63180329162329589c42de09e7971871.tar.gz";
      sha256 = "05984l61zfx9i5jp877laia0kica4dv9rg0m0540n5nwn8d73afp";
    }) {};
  packageOverrides = self: super: {
      pint = super.pint.overridePythonAttrs (old: {
        version = "0.16.1";
        src = super.fetchPypi {
          pname = "Pint";
          version = "0.16.1";

          sha256 = "0fn81s2z804vsbg8vpwib4d1wn6q43cqrpqgnrw4j5h3w2d2wfnl";
        };
      });
      lark-parser = super.pint.overridePythonAttrs (old: {
        version = "0.11.3";
        src = super.fetchPypi {
          pname = "lark-parser";
          version = "0.11.3";

          sha256 = "1ggr4na5d8qdr0i7z94yq69vj731by78fzb1fhbgic4bm4aai772";
        };

        # failing (trivially, I think), but I want to upgrade to the nixpkgs version of lark-parser anyways
        doCheck = false;
      });
      msgpack-types = super.buildPythonPackage rec {
        pname = "msgpack-types";
        version = "0.1.0";

        src = pythonPkgs.fetchPypi {
          inherit pname version;
          sha256 = "1djj7banb49xxqmrd61i06b7qaxmjmcsxph7yhsz74ls3y94116f";
        };
      };
      pubchempy = super.buildPythonPackage rec {
        pname = "PubChemPy";
        version = "1.0.4";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "03pdv1hhvkz7m480bbsgi000fhdi23jhby2bfsr57c8ar4pxrs94";
        };

        doCheck = false;
        
        propagatedBuildInputs = [ ];
      };
      typed-argument-parser = super.buildPythonPackage rec {
        pname = "typed-argument-parser";
        version = "1.7.1";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "00nqx72l53wan0s10s01sgyjwrpki0ardjy731r4agcr7vhw7y5m";
        };

        # this executes no tests and fails
        # checkInputs = with pythonPkgs; [
        #   pytestCheckHook
        # ];
        doCheck = false;
        
        propagatedBuildInputs = with self; [ typing-inspect typing-extensions ];
      };
      karabo-bridge = super.buildPythonPackage rec {
        pname = "karabo_bridge";
        version = "0.6.1";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "1j9ij2gw9c30b9wjy1af7p2g7pazdmn9fv7jcgnjvpxy772hzid8";
        };
        
        checkInputs = with self; [
          pytestCheckHook
          h5py
          testpath
        ];

        propagatedBuildInputs = with self; [ msgpack pyzmq numpy msgpack-numpy ];
      };
      extra-data = super.buildPythonPackage rec {
        pname = "EXtra-data";
        version = "1.8.1";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "069gq1lnmv5bni8k7jllsr471gmkk43x447lzjz7k9y38ypsa9vw";
        };

        propagatedBuildInputs = with self; [
          matplotlib
          numpy
          pandas
          xarray
          h5py
          self.karabo-bridge
          psutil
        ];

        checkInputs = with self; [
          pytestCheckHook
          testpath
          dask
        ];

        preCheck = ''
      export PATH=$PATH:$out/bin
    '';
      };
      types-pyyaml = super.buildPythonPackage rec {
        pname = "types-PyYAML";
        version = "5.4.10";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "1bc1rwyn26vcibjbq4j9x9kss59vb99miiapm5gacy0zkwg477hx";
        };
      };
      pyqt5-stubs = super.buildPythonPackage rec {
        pname = "PyQt5-stubs";
        version = "5.15.2.0";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "1cmrpx9iaklrxgmnjzxnxia5n9sqpxzpdmfx1kxrgqigy1kfl3fw";
        };

        # there are tests, but they require pyqt-5.14 (despite the version being pyqt-5.15)
        # so just ignore for now
        doCheck = false;
      };
      kamzik3 = super.buildPythonPackage rec {
        pname = "kamzik3";
        version = "0.7.7";

        src = super.fetchPypi {
          inherit pname version;
          sha256 = "1zs5hpaphd5f8y2jl7qr4r09n2gx59v0qnv0ym1ia9d1pp6i4qlr";
        };

        propagatedBuildInputs = with self; [
          numpy
          pyzmq
          bidict
          pyqt5
          pyserial
          pyqtgraph
          oyaml
          psutil
          natsort
          reportlab
          pandas
          tables
          h5py
          pint
        ];

        checkInputs = with self; [
          pytestCheckHook
          pytest-lazy-fixture
        ];
      };
  };
  python = pkgs.python3.override { inherit packageOverrides; };
  pythonPkgs = python.pkgs;
in pythonPkgs.buildPythonApplication rec {
    pname = "amarcord-gui";
    version = "1.0";
    format = "pyproject";
    
    src = pkgs.lib.cleanSource ./.;

    nativeBuildInputs = [ pkgs.poetry pkgs.qt5.wrapQtAppsHook ];
    
    propagatedBuildInputs = with pythonPkgs; [
      sqlalchemy
      extra-data
      pymysql
      cryptography
      alembic
      isodate
      bcrypt
      lark-parser
      pyyaml
      xdg
      typed-argument-parser
      pydantic
      numpy
      pyqt5
      pyqt5-stubs
      pyzmq
      pandas
      pubchempy
      matplotlib
      pint
      karabo-bridge
      msgpack
      msgpack-types
      extra-data
      kamzik3
    ];

    checkInputs = with pythonPkgs; [
      black
      pylint
      mypy
      pyfakefs
      pytest-qt
    ];

    dontWrapQtApps = true;
    preFixup = ''
      wrapQtApp "$out/bin/amarcord-gui"
    '';
  }
