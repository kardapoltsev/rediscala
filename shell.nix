{ pkgs ? import <nixpkgs> { } }:

pkgs.mkShell {
  name = "rediscala-shell";
  buildInputs = with pkgs; [ redis scala sbt ];
}
