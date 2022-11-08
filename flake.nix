{
  description = "abq";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        craneLib = crane.lib.${system};

        certFilter = path: _type: (builtins.match ".*abq_utils/data/cert/server.crt$" path) != null;
        certOrCargo = path: type:
          (certFilter path type) || (craneLib.filterCargoSources path type);

        buildInputs = if pkgs.stdenv.isDarwin then [ pkgs.libiconv pkgs.darwin.apple_sdk.frameworks.Security ] else [ ];
        nativeBuildInputs = [ pkgs.git ];

        abq = craneLib.buildPackage
          {
            cargoToml = ./crates/abq_cli/Cargo.toml;
            src = nixpkgs.lib.cleanSourceWith {
              src = ./.;
              filter = certOrCargo;
            };
            buildInputs = buildInputs;
            nativeBuildInputs = nativeBuildInputs;
            doCheck = false;
          };
      in
      {
        checks = {
          inherit abq;
        };

        packages.default = abq;

        apps.default = flake-utils.lib.mkApp {
          drv = abq;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = builtins.attrValues self.checks;

          # Extra inputs can be added here
          nativeBuildInputs = with pkgs; [
            cargo
            rustc
          ] ++ nativeBuildInputs ++ buildInputs;
        };

        formatter = pkgs.nixpkgs-fmt;
      });
}
