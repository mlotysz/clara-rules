(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def lib 'com.cerner/clara-rules)
(def version "0.24.2-SNAPSHOT")
(def class-dir "target/classes")

(defn- resolve-version [opts]
  (or (:version opts)
      (str/trim (b/git-process {:git-command ["describe" "--tags" "--abbrev=0"]}))))

(defn- jar-file [ver]
  (format "target/%s-%s.jar" (name lib) ver))

(defn- basis
  ([] (basis nil))
  ([aliases]
   (b/create-basis (cond-> {:project "deps.edn"}
                     aliases (assoc :aliases aliases)))))

(defn clean [_]
  (b/delete {:path "target"}))

(defn javac-main
  "Compile production Java sources to target/classes."
  [_]
  (b/javac {:src-dirs ["src/main/java"]
            :class-dir class-dir
            :basis (basis)
            :javac-opts ["--release" "21"]}))

(defn javac-test
  "Compile test Java sources to target/classes."
  [_]
  (b/javac {:src-dirs ["src/test/java"]
            :class-dir class-dir
            :basis (basis [:dev])
            :javac-opts ["--release" "21"]}))

(defn javac
  "Compile all Java sources (main + test)."
  [_]
  (javac-main nil)
  (javac-test nil))

(defn jar
  "Build the library JAR. Accepts optional :version override."
  [opts]
  (let [ver (or (:version opts) version)
        jf  (jar-file ver)]
    (clean nil)
    (javac-main nil)
    (let [b (basis)]
      (b/write-pom {:class-dir class-dir
                    :lib lib
                    :version ver
                    :basis b
                    :src-dirs ["src/main/clojure"]
                    :scm {:url "https://github.com/cerner/clara-rules"
                          :connection "scm:git:https://github.com/cerner/clara-rules.git"
                          :developerConnection "scm:git:ssh://git@github.com/cerner/clara-rules.git"
                          :tag (str "v" ver)}
                    :pom-data
                    [[:licenses
                      [:license
                       [:name "Apache License Version 2.0"]
                       [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]
                     [:developers
                      [:developer
                       [:id "rbrush"]
                       [:name "Ryan Brush"]
                       [:url "http://www.clara-rules.org"]]]]})
      (b/copy-dir {:src-dirs ["src/main/clojure" "clj-kondo"]
                   :target-dir class-dir})
      (b/jar {:class-dir class-dir
              :jar-file jf}))))

(defn install
  "Install JAR to local Maven repository. Accepts optional :version override."
  [opts]
  (let [ver (or (:version opts) version)
        jf  (jar-file ver)]
    (jar opts)
    (b/install {:basis (basis)
                :lib lib
                :version ver
                :jar-file jf
                :class-dir class-dir})))

(defn release
  "Tag HEAD and install JAR to local Maven repository.
  Version defaults to latest git tag; override with :version.
  Usage: clojure -T:build release
         clojure -T:build release :version '\"1.2.3\"'"
  [opts]
  (let [ver (resolve-version opts)
        tag (str "v" ver)]
    (println (str "Tagging " tag "..."))
    (b/process {:command-args ["git" "tag" tag]})
    (println (str "Installing " lib " " ver " to local Maven..."))
    (install (assoc opts :version ver))
    (println "Done.")))
