(defproject ratis "0.1.0-SNAPSHOT"
  :description "Ratis is a Redis proxy"
  :url "http://github.com/SupermanScott/ratis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-yaml "0.4.0"]
                 [aleph "0.3.0-beta7"]]
  :jvm-opts ["-server" "-XX:+UseConcMarkSweepGC" "-Xmx4g"]
  :test-selectors {:default #(not (some #{:benchmark :redis}
                                        (cons (:tag %) (keys %))))
                   :integration :redis
                   :benchmark :benchmark
                   :all (constantly true)}
  :main ratis.core)
