with import (fetchTarball https://github.com/NixOS/nixpkgs/archive/06c576b0525da85f2de86b3c13bb796d6a0c20f6.tar.gz) {};

let

  bundler = bundlerEnv {
    name = "rediscala-ruby";
    gemfile = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset = ./gemset.nix;
    inherit ruby;
  };

  redis-trib = stdenv.mkDerivation {
    name = "redis-trib.rb";
    inherit (redis) src;

    buildCommand = ''
      unpackPhase
      cd $sourceRoot
      mkdir -p $out/bin
      cp src/redis-trib.rb $out/bin
      patchShebangs $out/bin
    '';

    buildInputs = [ ruby ];
  };

in

stdenv.mkDerivation rec {
  name = "rediscala-dev";
  src = null;

  buildInputs = [ scala sbt redis ruby redis-trib bundler ];

  shellHook = ''
    export REDIS_TRIB_DIR="${redis-trib}/bin"
  '';
}
