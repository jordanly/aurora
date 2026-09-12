# Original Aurora UI build

The restored build uses Node 24.21.0 (Linux ARM64) and its bundled npm 11.19.0.
The root build launcher owns the archive pin. With that Node installation on
`PATH`, run from this directory:

```sh
npm ci
npm run lint
npm test -- --runInBand
npm run build
```

`package-lock.json` locks the complete dependency graph, including the local
`plugin` package. When changing custom plugin dependencies, update
`plugin/package.json` and regenerate the root UI lockfile with `npm install
--package-lock-only`; commit both manifests and the lockfile as appropriate.
There is no separate unlocked plugin installation step.

The checked-in graph needs no dependency installation hooks. `.npmrc` disables
them: the applicable hooks only print dependency banners, and the native
`fsevents` hooks belong to optional macOS packages. Custom dependencies that
require installation hooks need separate review and qualification.

Webpack 4.47.0 includes its own MD4 support for modern Node versions, so this
build needs no legacy OpenSSL flag. Production mode is explicit, matching the
previous webpack default. Enzyme's Cheerio dependency is pinned to
`1.0.0-rc.12`, which retains the CommonJS API used by the original Enzyme/Jest
combination. React 16 and the existing application routes remain in place.

Qualification on this toolchain passed all 33 original Jest suites (144 tests),
lint, and production bundling. The bundle and source map are generated at
`../src/main/resources/scheduler/assets/js/`. Existing bundle-size warnings
remain visible. These checks do not qualify browser/server integration or
modernize the retained legacy frontend dependencies.

References: [Node release status](https://nodejs.org/en/about/previous-releases),
[Node archive checksums](https://nodejs.org/dist/v24.21.0/SHASUMS256.txt), and
[webpack 4.47.0 release notes](https://github.com/webpack/webpack/releases/tag/v4.47.0).
