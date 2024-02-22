(Frontend)=
# Frontend

## Description

The web frontend is a [single-page application](https://en.wikipedia.org/wiki/Single-page_application) (SPA) written in [Elm](https://elm-lang.org/), a delightful language for reliable web applications.

## Learning Elm

The best place to start learning Elm is the [official guide](https://guide.elm-lang.org/install/elm.html). From there, you can dive into the source code directly.

## Understanding the code

The code starts in `src/Main.elm`, and specifically in the `main` function. There is one [Elm Port](https://guide.elm-lang.org/interop/ports) to access local storage, since that's not in the Elm standard yet. There is also one [Custom Element](https://guide.elm-lang.org/interop/custom_elements) in `src/Ports.elm` (and referenced in `src/index.html`) to implement the [UglyMol](http://uglymol.github.io/) electron density viewer (in `uglymol-custom-element.js`).

### UglyMol

UglyMol is not an Elm application. Instead, it is a JavaScript library that's available for download externally. Since we didn't want to include its source code verbatim into AMARCORD, there is a script `frontend/download-uglymol-dependencies.sh` which downloads all the files you need into the `output` directory, and also puts `uglymol-custom-element.js`, our Elm-to-JS glue code, into that directory. With this in place, you can use UglyMol from Elm via custom element code a la `node "uglymol-viewer"` (see [Custom Element](https://guide.elm-lang.org/interop/custom_elements) in the Elm guide for more information on the specifics).

### Build pipeline

To build the frontend using [Nix](https://nixos.org/), we use the excellent [mkElmDerivation](https://github.com/jeslie0/mkElmDerivation) flake.

To integrate UglyMol into the build pipeline, we have a [separate repository](https://gitlab.desy.de/cfel-sc-public/uglymol) on the DESY GitLab instance. This also sports a `flake.nix`, making integration in the AMARCORD `flake.nix` very easy.

In AMARCORD's flake, we have a `amarcord-frontend` derivation in which we `fetchurl` some of the additional dependencies (for example, to load `mtz` files), and then copy all of that into the output directory, alongside the result of building the main Elm application.

### `Main.elm` Structure

### `elm-review`

To ensure code quality in the frontend, we use the excellent [elm-review](https://package.elm-lang.org/packages/jfmengels/elm-review/latest/). To run it, just `cd` into `frontend` and do `npx elm-review` (here, we need NodeJS). It will show all the errors it finds. Note that in many cases, it can even correct mistakes. Try running it with the `--fix` or `--fix-all` command-line option!

The configuration for `elm-review` resides in `frontend/review/src/ReviewConfig.elm` (and as you can see, `elm-review` lives under a completely separate directory structure `frontent/review`).

### API

To call out to the web server, we use code generated from the OpenAPI specification, which resides under `frontend/generated/API`. See the [](OpenAPI) part of the documentation for more information.

Note that the generated OpenAPI code is actually committed into the source code repository to make working with the code easier. It is also checked in CI to ensure nobody forgets to regenerate it after changes to the web server (see the `frontend-check-openapi` job).

## How to build and start the frontend

### Production build

The frontend is written in [Elm](https://elm-lang.org/). If you are not a fan of NodeJS, you can download the `elm` binary at the [Install Elm](https://guide.elm-lang.org/install/elm.html) web site and build the frontend using that binary only. There is a script in `frontend/build-elm-manually.sh` that does just that. It puts all the assets and the generated `.js` file into `frontend/build-output`. You can then deploy that somewhere and host it behind a reverse proxy to check out AMARCORD. Note that you have to manually take care of UglyMol in this case.

### Live build

To run a *live development environment*, you need [elm-live](https://github.com/wking-io/elm-live) which, unfortunately, needs [NodeJS](https://nodejs.org/en/). But you can install that really easily on different platforms.

Assuming you have got it installed, run:

```
npm install
```

To install the dependencies (you can also use [Yarn](https://yarnpkg.com/), which might be more performant). Then, start a development server via:

```
./run-live-dev-env.sh
```

And point your browser to <http://localhost:8001>. Internally, this proxies requests going to `/api` to `localhost:5001`. So be sure to start your backend on that port.
