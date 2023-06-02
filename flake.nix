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

        assertVersion = version: pkg: (
          assert (pkgs.lib.assertMsg (builtins.toString pkg.version == version) ''
            Expecting version of ${pkg.name} to be ${version} but got ${pkg.version};
          '');
          pkg
        );
        buildInputs =
          if pkgs.stdenv.isDarwin then [
            (assertVersion "50" pkgs.libiconv)
            (assertVersion "11.0.0" pkgs.darwin.apple_sdk.frameworks.Security)
          ] else [ ];

        nativeBuildInputs = [ (assertVersion "2.38.1" pkgs.git) ];

        abq =
          craneLib.buildPackage
            {
              cargoToml = ./crates/abq_cli/Cargo.toml;
              src = nixpkgs.lib.cleanSourceWith {
                src = ./.;
                filter = certOrCargo;
              };
              buildInputs = buildInputs;
              nativeBuildInputs = nativeBuildInputs;
              doCheck = false;
              NIX_ABQ_VERSION = "0.${self.lastModifiedDate}.0+g${self.shortRev or "dirty"}";
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

        # note, we have a dev shell working, but rust-analyzer doesn't totally work because of
        # https://github.com/rust-lang/rust-analyzer/issues/13393
        # so I wouldn't recommend using the nix dev shell until that's fixed
        devShells.default = pkgs.mkShell {
          inputsFrom = builtins.attrValues self.checks;
          # see: https://discourse.nixos.org/t/rust-src-not-found-and-other-misadventures-of-developing-rust-on-nixos/11570/3
          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";


          # Extra inputs can be added here
          nativeBuildInputs = with pkgs; [
            (assertVersion "1.65.0" cargo)
            (assertVersion "1.65.0" rustc)
            (assertVersion "2022-12-05" rust-analyzer)
          ] ++ nativeBuildInputs ++ buildInputs;
        };

        formatter = pkgs.nixpkgs-fmt;
      });
}
