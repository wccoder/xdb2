(defsystem :xdb2-tests
  :serial t
  :pathname "tests/"
  :components ((:file "tests"))
  :depends-on (xdb2 rt))
