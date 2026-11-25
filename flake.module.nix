{ inputs, lib, ... }:
{
  perSystem = { pkgs, ... }:
    let
      registry = inputs.haskell-nix.lib.mkFirstPartyPackageSet {
        packageSet = "default";
        channel = "hackage";
      };
    in
    {
      haskellProject.applicationOverlays = lib.mkAfter [
        (registry.haskellExtension pkgs.haskell.lib.compose pkgs)
        (final: prev: {
          # The upstream test component requires QuickCheck 2.15.
          shibuya-core = pkgs.haskell.lib.dontCheck (final.callHackageDirect
            {
              pkg = "shibuya-core";
              ver = "0.13.0.1";
              sha256 = "1wc1zvi6jksa35qvdni6p88mi79nfmy6yc3j3yvn8sw0kw0wi057";
            }
            { });
          # Upstream runs its own multi-server cluster and Redis module suites.
          hedis = pkgs.haskell.lib.dontCheck (final.callHackageDirect
            {
              pkg = "hedis";
              ver = "0.16.4";
              sha256 = "1grr01yq0drrjcy0dlfng6f6vcxv6nvfs8q2rvpb0jm3q0c1d1qz";
            }
            {
              resource-pool = final.callHackageDirect
                {
                  pkg = "resource-pool";
                  ver = "0.5.1.0";
                  sha256 = "1kw2arc57cyiyr1q38dxn0mrx2pxc8cbpny0m1c4zp7vphab0rr5";
                }
                { };
            });
          shibuya-redis-adapter = pkgs.haskell.lib.addTestToolDepend prev.shibuya-redis-adapter pkgs.redis;
        })
      ];
    };
}
