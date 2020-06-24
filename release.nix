{ nixpkgs ? import <nixpkgs> {}
}:
let
  package = import ./default.nix {
    inherit nixpkgs;
  };

  image = package.image {};
  imagePlus = package.image { additionalContents = with nixpkgs; [ bash coreutils google-cloud-sdk ]; };

  # script to push images to registry
  # depends on runtime vars DOCKERHUB_USERNAME, DOCKERHUB_PASSWORD, and DOCKERHUB_IMAGE
  pushImagesScript = nixpkgs.writeScript "push-images" ''
    #!${nixpkgs.stdenv.shell} -e

    ${nixpkgs.skopeo}/bin/skopeo --insecure-policy copy --dest-creds $DOCKERHUB_USERNAME:$DOCKERHUB_PASSWORD docker-archive:${image} docker://$DOCKERHUB_IMAGE
    ${nixpkgs.skopeo}/bin/skopeo --insecure-policy copy --dest-creds $DOCKERHUB_USERNAME:$DOCKERHUB_PASSWORD docker-archive:${imagePlus} docker://$DOCKERHUB_IMAGE:plus
  '';

in with package; {
  inherit bins image imagePlus pushImagesScript;
  touch = bins // {
    inherit image imagePlus pushImagesScript;
  };
}
