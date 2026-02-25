This is a history of changes to clara-rules.

For the upstream changelog (versions 0.24.0 and earlier), see the
[oracle-samples/clara-rules](https://github.com/oracle-samples/clara-rules) repository.

# 0.25.0-SNAPSHOT

Fork-specific changes (based on upstream 0.24.0):

* Migrate build system from Leiningen to Clojure CLI (deps.edn + tools.build)
* Target Java 21+
* Upgrade dependencies: Clojure 1.12.4, ClojureScript 1.12.101, Prismatic Schema 1.4.1
* Replace Puppeteer-based CLJS tests with Node.js target
* Fix javac compilation warnings (use `--release` flag instead of `-source`/`-target`)
* Fix CLJS compilation warning in `testing_utils.cljc` (missing `cljs.test` require)
* Fix clj-kondo errors for `defrule`/`defquery` docstrings in hooks
* Remove Oracle-specific files (CONTRIBUTING.md, SECURITY.md, RELEASE.md)
* Convert README to AsciiDoc
