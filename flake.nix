{
  description = "clara-rules development shell — Clojure on GraalVM CE (latest) + Node for CLJS tests";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      jdk = pkgs.graalvmPackages.graalvm-ce;
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          jdk
          pkgs.clojure
          pkgs.rlwrap
          pkgs.babashka
          pkgs.clj-kondo
          pkgs.clojure-lsp
          pkgs.nodejs_22
        ];

        shellHook = ''
          export JAVA_HOME="${jdk}/lib/openjdk"
          echo "clara-rules shell: $(java -version 2>&1 | head -1), clojure $(clojure --version 2>&1 | head -1 | awk '{print $NF}'), node $(node --version)"
        '';
      };
    };
}
