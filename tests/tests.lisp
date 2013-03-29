(defpackage :xdb2-tests
  (:use :cl :rt :xdb2))

(in-package :xdb2-tests)

(defvar *test-db* nil)
(defvar *test-col* nil)

(defun make-new-db ()
  (when (probe-file "/tmp/xdb-test/test.log")
    (delete-file "/tmp/xdb-test/test.log"))
  (setf *test-db* (make-instance 'xdb :location "/tmp/xdb-test/")
        *test-col* (add-collection *test-db* "test")))

(defun load-collection ()
  (setf *test-db* (make-instance 'xdb :location "/tmp/xdb-test/")
        *test-col* (add-collection *test-db* "test")))

(defun clear-class-cache (class-name)
  (let ((class (find-class class-name)))
    (setf (xdb2::id-cache class) (make-hash-table :size 1000)
          (xdb2::last-id class) 0
          (version class) 0)))

(defclass test ()
  ())

(defclass test-versioned (test)
  ((x :initarg :x
      :initform nil
      :accessor x)) 
  (:metaclass storable-versioned-class))

(defmethod doc-collection ((doc test))
  *test-col*)

(deftest versioning.write
  (progn
    (make-new-db)
    (clear-class-cache 'test-versioned)
    (let ((object (make-instance 'test-versioned
                                 :top-level t)))
      (assert (= (version object) 1))
      (assert (not (old-versions object)))
      (assert (not (written object)))
      (persist object)
      (assert (= (version object) 1))
      (assert (not (old-versions object)))
      (assert (written object))
      t))
  t)

(deftest versioning.read
  (progn
    (clear-class-cache 'test-versioned)
    (load-collection)
    (assert (= (length (docs *test-col*)) 1))
    (let ((object (alexandria:first-elt (docs *test-col*))))
      (assert (= (version object) 1))
      (assert (not (old-versions object)))
      (assert (written object)))
    t)
  t)

(deftest versioning-new-version.write
  (progn
    (let ((object (alexandria:first-elt (docs *test-col*))))
      (setf (slot-value object 'x) 10)
      (old-versions object)
      (assert (= (version object) 2))
      (assert (= (length (old-versions object)) 1))
      (assert (not (written object)))
      (persist object)
      (assert (written object))
      t))
  t)

(deftest versioning-new-version.read
  (progn
    (clear-class-cache 'test-versioned)
    (load-collection)
    (assert (= (length (docs *test-col*)) 1))
    (let ((object (alexandria:first-elt (docs *test-col*))))
      (assert (= (version object) 2))
      (assert (= (length (old-versions object)) 1))
      (assert (written object))
      t))
  t)

(deftest clear-old-versions.write
  (let ((doc (alexandria:first-elt (docs *test-col*))))
    (assert (old-versions doc))
    (clear-old-versions doc)
    (assert (null (old-versions doc)))
    (assert (= (version doc) 1))
    (assert (written doc))
    t)
  t)

(deftest clear-old-versions.read
  (progn
    (clear-class-cache 'test-versioned)
    (load-collection)
    (let ((doc (alexandria:first-elt (docs *test-col*))))
      (assert (null (old-versions doc)))
      (assert (= (version doc) 1))
      (assert (written doc))
      t))
  t)

(deftest class-redefinition-add-slot.write
  (progn
    (make-new-db)
    (defclass test-class (test)
      ((x :initarg :x
          :initform nil
          :accessor x)) 
      (:metaclass storable-versioned-class))
    (clear-class-cache 'test-class)
    (persist (make-instance 'test-class :x 1 :top-level t))
    (defclass test-class (test)
      ((y :initarg :y
          :initform nil
          :accessor y)
       (x :initarg :x
          :initform nil
          :accessor x)) 
      (:metaclass storable-versioned-class))
    (persist (make-instance 'test-class :x 2 :y 3 :top-level t))
    t)
  t)

(deftest class-redefinition-add-slot.read
  (progn
    (clear-class-cache 'test-class)
    (load-collection)
    (let ((doc (elt (docs *test-col*) 1)))
      (and (eql (x doc) 2)
           (eql (y doc) 3))))
  t)

(deftest class-redefinition-remove-slot.read
  (progn
    (clear-class-cache 'test-class)
    (defclass test-class (test)
      ((y :initarg :y
          :initform nil
          :accessor y)) 
      (:metaclass storable-versioned-class))
    (load-collection)
    (let ((doc0 (elt (docs *test-col*) 0))
          (doc1 (elt (docs *test-col*) 1)))
      (and (eql (y doc0) nil)
           (eql (y doc1) 3)))
    t)
  t)

(deftest metaclass-change.write
  (progn
    (make-new-db)
    (defclass test-class2 (test)
      ((y :initarg :y
          :initform nil
          :accessor y)) 
      (:metaclass storable-versioned-class))
    (clear-class-cache 'test-class2)
    (let ((object (make-instance 'test-class2 :y 1 :top-level t) ))
      (persist object)
      (incf (y object))
      (persist object)
      (incf (y object))
      (persist object)
      (assert (= (version object) 3)))
    t)
  t)

(deftest metaclass-change.read
  (progn
    (setf (find-class 'test-class2) nil)
    (defclass test-class2 (test)
      ((y :initarg :y
          :initform nil
          :accessor y)) 
      (:metaclass storable-class))
    (load-collection)
    (let ((object (alexandria:first-elt (docs *test-col*))))
      (assert (not (typep object 'storable-versioned-object)))
      (y object)))
  3)

(deftest metaclass-change.read.2
  (progn
    (setf (find-class 'test-class2) nil)
    (defclass test-class2 (test)
      ((y :initarg :y
          :initform nil
          :accessor y)) 
      (:metaclass storable-versioned-class))
    (load-collection)
    (let ((object (alexandria:first-elt (docs *test-col*))))
      (assert (typep object 'storable-versioned-object))
      (assert (= (version object) 3))
      (y object)))
  3)
