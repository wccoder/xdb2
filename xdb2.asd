;;;; xdb.asd

(asdf:defsystem #:xdb2
  :serial t
  :depends-on (closer-mop ieee-floats alexandria
                          bordeaux-threads)
  :components ((:file "package")
               #+(and sbcl (or x86 x86-64))
               (:file "io-sbcl")
               #-(and sbcl (or x86 x86-64))
               (:file "io-generic")
               (:file "mop")
               (:file "disk")
               (:file "common")
               (:file "xdb")
               (:file "db-functions")
               (:file "document")))

(defmethod perform ((o test-op) (c (eql (find-system :xdb2))))
  (operate 'load-op :xdb2)
  (funcall (intern (symbol-name :do-tests) (find-package :rt))))
