let
  pkgs = import <nixpkgs> {};
  pythonPkgs = pkgs.python3Packages;
  karabo-bridge = pythonPkgs.buildPythonPackage rec {
    pname = "karabo_bridge";
    version = "0.6.0";

    src = pythonPkgs.fetchPypi {
      inherit pname version;
      sha256 = "0q3msk432hbyq14v2bfg4imbrzi2cgvhvb20hi1dv0kgilfyjr32";
    };

    doCheck = false;

    propagatedBuildInputs = with pythonPkgs; [ msgpack pyzmq numpy msgpack-numpy ];
  };
  lark-parser = pythonPkgs.buildPythonPackage rec {
    pname = "lark-parser";
    version = "0.11.2";

    src = pythonPkgs.fetchPypi {
      inherit pname version;
      sha256 = "0sxvg09xrp68i2g79926v2nzjfcyhx4rsz9k5x847cpjxdhh8qgg";
    };

    doCheck = false;
    
    propagatedBuildInputs = [ ];
  };
  pubchempy = pythonPkgs.buildPythonPackage rec {
    pname = "PubChemPy";
    version = "1.0.4";

    src = pythonPkgs.fetchPypi {
      inherit pname version;
      sha256 = "03pdv1hhvkz7m480bbsgi000fhdi23jhby2bfsr57c8ar4pxrs94";
    };

    doCheck = false;
    
    propagatedBuildInputs = [ ];
  };
  amarcord-daemon = pythonPkgs.buildPythonPackage rec {
    pname = "amarcord-daemon";
    version = "1.0";

    src = ./.;

    doCheck = false;
  
    propagatedBuildInputs = [
      lark-parser
      pythonPkgs.isodate
      pythonPkgs.bcrypt
      pythonPkgs.sqlalchemy
      karabo-bridge
    ];

  };
in {
  daemon = amarcord-daemon;
  daemon-docker = pkgs.dockerTools.buildImage {
    name = "amarcord-daemon";
    tag = "latest";

    contents = amarcord-daemon;

    config = {
      Cmd = [ "/bin/amarcord-daemon" ];
    };
  };
}
