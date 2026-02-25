(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'com.cerner/clara-rules)
(def version "0.24.1-SNAPSHOT")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

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
  "Build the library JAR."
  [_]
  (clean nil)
  (javac-main nil)
  (let [b (basis)]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis b
                  :src-dirs ["src/main/clojure"]
                  :scm {:url "https://github.com/cerner/clara-rules"
                        :connection "scm:git:https://github.com/cerner/clara-rules.git"
                        :developerConnection "scm:git:ssh://git@github.com/cerner/clara-rules.git"
                        :tag (str "v" version)}
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
            :jar-file jar-file})))

(defn install
  "Install JAR to local Maven repository."
  [_]
  (jar nil)
  (b/install {:basis (basis)
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))
