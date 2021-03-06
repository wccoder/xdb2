;;; -*- Mode: Lisp -*-

(in-package #:xdb2)

(defclass collection ()
  ((name :initarg :name
         :initform nil
         :accessor name)
   (path :initarg :path
         :initform nil
         :accessor path)
   (docs :initarg :docs
         :initform nil
         :accessor docs)
   (packages :initform (make-s-packages)
             :accessor packages)
   (classes :initform (make-class-cache)
            :accessor classes)
   (last-id :initform 0
            :accessor last-id)
   (object-cache :initform (make-hash-table :size 1000
                                            :test 'eq)
                 :accessor object-cache)
   (id-cache :initarg :id-cache
             :initform (make-hash-table :size 1000)
             :accessor id-cache)
   (loaded :initarg :loaded
           :initform nil
           :accessor loaded)
   (lock :initform (bt:make-lock)
         :accessor lock)))

(defmethod print-object ((object collection) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (princ (name object) stream)))

(defmethod print-object ((object collection) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (when (slot-boundp object 'name)
      (princ (name object) stream))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter *codes*
    #(ascii-string
      storable-link
      cons
      string
      null
      storable-class
      storable-object
      standard-class
      standard-object
      standard-link
      fixnum
      bignum
      ratio
      double-float
      single-float
      complex
      symbol
      intern-package-and-symbol
      intern-symbol
      character
      simple-vector
      array
      hash-table
      pathname
      collection
      delete-marker
      clear-version-marker)))

(defvar *statistics* ())
(defun collect-stats (code)
  (let* ((type (aref *codes* code))
         (cons (assoc type *statistics*)))
    (if cons
        (incf (cdr cons))
        (push (cons type 1) *statistics*))
    type))

(defvar *collection* nil)
(defvar *storable-object-hook* nil)
(defvar *storable-object-delete-hook* nil)

(defvar *classes*)
(defvar *packages*)

(declaim (vector *classes* *packages*))

(defvar *indexes*)
(defvar *object-cache*)
(declaim (hash-table *indexes*
                     *object-cache*))

(defvar *db*)
(defvar *import* nil)
(defvar *export* nil)
(defvar *snapshot* nil)
(defvar *export-last-id*)

(defvar *written-objects*)
(declaim (hash-table *written-objects*))

(defvar *export-test-function*)
(declaim (function *export-test-function*))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun type-code (type)
    (position type *codes*)))

(defparameter *readers* (make-array (length *codes*)))
(declaim (type (simple-array function (*)) *readers*))

(defmacro defreader (type args &body body)
  (let ((name (intern (format nil "~a-~a" type '#:reader))))
    `(progn
       (defun ,name ,args
         ,@body)
       (setf (aref *readers* ,(type-code type))
             #',name))))

(declaim (inline call-reader))
(defun call-reader (code stream)
  ;; (collect-stats code)
  (funcall (aref *readers* code) stream))

;;;

(defconstant +sequence-length+ 2)
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defconstant +fixnum-length+ 4))
(defconstant +char-length+ 2)
(defconstant +id-length+ 4)
(defconstant +class-id-length+ 2)
(defconstant +hash-table-length+ 3)

(defconstant +unbound-slot+ 254)
(defconstant +end+ 255)

(defconstant +ascii-char-limit+ (code-char 128))

(deftype ascii-string ()
  '(or
    #+sb-unicode simple-base-string ; on #-sb-unicode the limit is 255
    (satisfies ascii-string-p)))

(defun ascii-string-p (string)
  (declare (simple-string string))
  (loop for char across string
        always (char< char +ascii-char-limit+)))

(deftype storage-fixnum ()
  `(signed-byte ,(* +fixnum-length+ 8)))

(defun make-class-cache ()
  (make-array 10 :adjustable t :fill-pointer 0))

(defmacro with-collection (collection &body body)
  (let ((collection-sym (gensym)))
    `(let* ((,collection-sym ,collection)
            (*collection* ,collection-sym)
            (*packages* (packages ,collection-sym))
            (*classes* (classes ,collection-sym))
            (*indexes* (id-cache ,collection-sym))
            (*object-cache* (object-cache ,collection-sym))
            (*inhibit-change-marking* t))
       ,@body)))

(defun is-written (object)
  (cond (*export*
         (gethash object *written-objects*))
        (*snapshot*
         (or (gethash object *written-objects*)
             (not (eq (collection object) *collection*))))
        (t
         (written object))))

(defun set-written (object)
  (if (or *export* *snapshot*)
      (setf (gethash object *written-objects*) t)
      (setf (written object) t)))

;;;

(defun slot-effective-definition (class slot-name)
  (find slot-name (class-slots class) :key #'slot-definition-name))

(defgeneric write-object (object stream))

(defun dump-data (stream)
  (map-docs
   nil
   (lambda (document)
     (write-top-level-object document stream))
   *collection*))

(defun write-top-level-object (object stream)
  (typecase object
    (storable-versioned-object
     (unless (or *export* *snapshot*)
       (write-objects-inside-slots object stream))
     (write-storable-versioned-object object stream))
    (storable-object
     (unless (or *export* *snapshot*)
       (write-objects-inside-slots object stream))
     (write-storable-object object stream))
    (t
     (write-object object stream))))

(declaim (inline read-next-object))
(defun read-next-object (stream)
  (call-reader (read-n-bytes 1 stream) stream))

;;; NIL

(defmethod write-object ((object null) stream)
  (write-n-bytes #.(type-code 'null) 1 stream))

(defreader null (stream)
  (declare (ignore stream))
  nil)

;;; Symbol

(defun make-s-packages ()
  (make-array 10 :adjustable t :fill-pointer 0))

(defun make-s-package (package)
  (let ((symbols (make-array 100 :adjustable t :fill-pointer 0)))
    (values (vector-push-extend (cons package symbols) *packages*)
            symbols
            t)))

(defun find-s-package (package)
  (loop for i below (length *packages*)
        for (stored-package . symbols) = (aref *packages* i)
        when (eq package stored-package)
        return (values i symbols)
        finally (return (make-s-package package))))

(defun s-intern (symbol)
  (multiple-value-bind (package-id symbols new-package)
      (find-s-package (symbol-package symbol))
    (let* ((existing (and (not new-package)
                          (position symbol symbols)))
           (symbol-id (or existing
                          (vector-push-extend symbol symbols))))
      (values package-id symbol-id new-package (not existing)))))

(defun s-intern-existing (symbol symbols)
  (vector-push-extend symbol symbols))

(defmethod write-object ((symbol symbol) stream)
  (multiple-value-bind (package-id symbol-id
                        new-package new-symbol) (s-intern symbol)
    (cond ((and new-package new-symbol)
           (write-n-bytes #.(type-code 'intern-package-and-symbol) 1 stream)
           (write-object (package-name (symbol-package symbol)) stream)
           (write-object (symbol-name symbol) stream))
          (new-symbol
           (write-n-bytes #.(type-code 'intern-symbol) 1 stream)
           (write-n-bytes package-id +sequence-length+ stream)
           (write-object (symbol-name symbol) stream))
          (t
           (write-n-bytes #.(type-code 'symbol) 1 stream)
           (write-n-bytes package-id +sequence-length+ stream)
           (write-n-bytes symbol-id +sequence-length+ stream)))))

(defreader symbol (stream)
  (let* ((package-id (read-n-bytes +sequence-length+ stream))
         (symbol-id (read-n-bytes +sequence-length+ stream))
         (package (or
                   (and
                    (array-in-bounds-p *packages* package-id)
                    (aref *packages* package-id))
                   (error "Package with id ~a not found" package-id)))
         (symbol (and (array-in-bounds-p (cdr package) symbol-id)
                      (aref (cdr package) symbol-id))))
    (or symbol
        (error "Symbol with id ~a in package ~a not found."
               symbol-id (car package)))))

(defreader intern-package-and-symbol (stream)
  (let* ((package-name (read-next-object stream))
         (symbol-name (read-next-object stream))
         (package (or (find-package package-name)
                      (error "Package ~a not found" package-name)))
         (symbol (intern symbol-name package))
         (s-package (nth-value 1 (make-s-package package))))
    (s-intern-existing symbol s-package)
    symbol))

(defreader intern-symbol (stream)
  (let* ((package-id (read-n-bytes +sequence-length+ stream))
         (symbol-name (read-next-object stream))
         (package (or (aref *packages* package-id)
                      (error "Package with id ~a for symbol ~a not found"
                             package-id symbol-name)))
         (symbol (intern symbol-name (car package))))
    (s-intern-existing symbol (cdr package))
    symbol))

;;; Integer

(declaim (inline sign))
(defun sign (n)
  (if (minusp n)
      1
      0))

(defun write-fixnum (n stream)
  (declare (storage-fixnum n))
  (write-n-bytes #.(type-code 'fixnum) 1 stream)
  (write-n-signed-bytes n +fixnum-length+ stream))

(defun write-bignum (n stream)
  (declare ((and integer (not storage-fixnum)) n))
  (write-n-bytes #.(type-code 'bignum) 1 stream)
  (write-n-bytes (sign n) 1 stream)
  (let* ((fixnum-bits (* +fixnum-length+ 8))
         (n (abs n))
         (size (ceiling (integer-length n) fixnum-bits)))
    (write-n-bytes size 1 stream)
    (loop for position by fixnum-bits below (* size fixnum-bits)
          do
          (write-n-bytes (ldb (byte fixnum-bits position) n)
                         +fixnum-length+ stream))))

(defmethod write-object ((object integer) stream)
  (typecase object
    (storage-fixnum
     (write-fixnum object stream))
    (t (write-bignum object stream))))

(declaim (inline read-sign))
(defun read-sign (stream)
  (if (plusp (read-n-bytes 1 stream))
      -1
      1))

(defreader bignum (stream)
  (let ((fixnum-bits (* +fixnum-length+ 8))
        (sign (read-sign stream))
        (size (read-n-bytes 1 stream))
        (integer 0))
    (loop for position by fixnum-bits below (* size fixnum-bits)
          do
          (setf (ldb (byte fixnum-bits position) integer)
                (read-n-bytes +fixnum-length+ stream)))
    (* sign integer)))

(defreader fixnum (stream)
  (read-n-signed-bytes +fixnum-length+ stream))

;;; Ratio

(defmethod write-object ((object ratio) stream)
  (write-n-bytes #.(type-code 'ratio) 1 stream)
  (write-object (numerator object) stream)
  (write-object (denominator object) stream))

(defreader ratio (stream)
  (/ (read-next-object stream)
     (read-next-object stream)))

;;; Float

(defun write-8-bytes (n stream)
  (write-n-bytes (ldb (byte 32 0) n) 4 stream)
  (write-n-bytes (ldb (byte 64 32) n) 4 stream))

(defun read-8-bytes (stream)
  (logior (read-n-bytes 4 stream)
          (ash (read-n-bytes 4 stream) 32)))

(defmethod write-object ((float float) stream)
  (etypecase float
    (single-float
     (write-n-bytes #.(type-code 'single-float) 1 stream)
     (write-n-bytes (ieee-floats:encode-float32 float) 4 stream))
    (double-float
     (write-n-bytes #.(type-code 'double-float) 1 stream)
     (write-8-bytes (ieee-floats:encode-float64 float) stream))))

(defreader single-float (stream)
  (ieee-floats:decode-float32 (read-n-bytes 4 stream)))

(defreader double-float (stream)
  (ieee-floats:decode-float64 (read-8-bytes stream)))

;;; Complex

(defmethod write-object ((complex complex) stream)
  (write-n-bytes #.(type-code 'complex) 1 stream)
  (write-object (realpart complex) stream)
  (write-object (imagpart complex) stream))

(defreader complex (stream)
  (complex (read-next-object stream)
           (read-next-object stream)))

;;; Characters

(defmethod write-object ((character character) stream)
  (write-n-bytes #.(type-code 'character) 1 stream)
  (write-n-bytes (char-code character) +char-length+ stream))

(defreader character (stream)
  (code-char (read-n-bytes +char-length+ stream)))

;;; Strings

(defun write-ascii-string (string stream)
  (declare (simple-string string))
  (loop for char across string
        do (write-n-bytes (char-code char) 1 stream)))

(defun write-multibyte-string (string stream)
  (declare (simple-string string))
  (loop for char across string
        do (write-n-bytes (char-code char) +char-length+ stream)))

(defmethod write-object ((string string) stream)
  (when (> (length string) #.(1- (expt 2 (* 8 +sequence-length+))))
    (error "Strings can't be longer than ~a"
           #.(1- (expt 2 (* 8 +sequence-length+)))))
  (etypecase string
    ((not simple-string)
     (call-next-method))
    #+sb-unicode
    (simple-base-string
     (write-n-bytes #.(type-code 'ascii-string) 1 stream)
     (write-n-bytes (length string) +sequence-length+ stream)
     (write-ascii-string string stream))
    (ascii-string
     (write-n-bytes #.(type-code 'ascii-string) 1 stream)
     (write-n-bytes (length string) +sequence-length+ stream)
     (write-ascii-string string stream))
    (string
     (write-n-bytes #.(type-code 'string) 1 stream)
     (write-n-bytes (length string) +sequence-length+ stream)
     (write-multibyte-string string stream))))

(declaim (inline read-ascii-string))
(defun read-ascii-string (length stream)
  (let ((string (make-string length :element-type 'base-char)))
    ;#-sbcl
    (loop for i below length
          do (setf (schar string i)
                   (code-char (read-n-bytes 1 stream))))
    #+(and nil sbcl (or x86 x86-64))
    (read-ascii-string-optimized length string stream)
    string))

(defreader ascii-string (stream)
  (read-ascii-string (read-n-bytes +sequence-length+ stream) stream))

(defreader string (stream)
  (let* ((length (read-n-bytes +sequence-length+ stream))
         (string (make-string length :element-type 'character)))
    (loop for i below length
          do (setf (schar string i)
                   (code-char (read-n-bytes +char-length+ stream))))
    string))

;;; Pathname

(defmethod write-object ((pathname pathname) stream)
  (write-n-bytes #.(type-code 'pathname) 1 stream)
  (write-object (pathname-name pathname) stream)
  (write-object (pathname-directory pathname) stream)
  (write-object (pathname-device pathname) stream)
  (write-object (pathname-type pathname) stream)
  (write-object (pathname-version pathname) stream))

(defreader pathname (stream)
  (make-pathname
   :name (read-next-object stream)
   :directory (read-next-object stream)
   :device (read-next-object stream)
   :type (read-next-object stream)
   :version (read-next-object stream)))

;;; Cons

(defmethod write-object ((list cons) stream)
  (cond ((alexandria:circular-list-p list)
         (error "Can't store circular lists"))
        (t
         (write-n-bytes #.(type-code 'cons) 1 stream)
         (loop for cdr = list then (cdr cdr)
               do
               (cond ((consp cdr)
                      (write-object (car cdr) stream))
                     (t
                      (write-n-bytes +end+ 1 stream)
                      (write-object cdr stream)
                      (return)))))))

(defreader cons (stream)
  (let ((first-cons (list (read-next-object stream))))
    (loop for previous-cons = first-cons then new-cons
          for car = (let ((id (read-n-bytes 1 stream)))
                      (cond ((eq id +end+)
                             (setf (cdr previous-cons) (read-next-object stream))
                             (return))
                            ((call-reader id stream))))
          for new-cons = (list car)
          do (setf (cdr previous-cons) new-cons))
    first-cons))

;;; Simple-vector

(defmethod write-object ((vector vector) stream)
  (typecase vector
    (simple-vector
     (write-simple-vector vector stream))
    (t
     (call-next-method))))

(defun write-simple-vector (vector stream)
  (declare (simple-vector vector))
  (when (> (length vector) #.(1- (expt 2 (* 8 +sequence-length+))))
    (error "Vectors can't be longer than ~a"
           #.(1- (expt 2 (* 8 +sequence-length+)))))
  (write-n-bytes #.(type-code 'simple-vector) 1 stream)
  (write-n-bytes (length vector) +sequence-length+ stream)
  (loop for elt across vector
        do (write-object elt stream)))

(defreader simple-vector (stream)
  (let ((vector (make-array (read-n-bytes +sequence-length+ stream))))
    (loop for i below (length vector)
          do (setf (svref vector i) (read-next-object stream)))
    vector))

;;; Array

(defun boolify (x)
  (if x
      1
      0))

(defmethod write-object ((array array) stream)
  (write-n-bytes #.(type-code 'array) 1 stream)
  (when (> (array-total-size array) #.(1- (expt 2 (* 8 +sequence-length+))))
    (error "Arrays can't be larger than ~a"
           #.(1- (expt 2 (* 8 +sequence-length+)))))
  (write-object (array-dimensions array) stream)
  (cond ((array-has-fill-pointer-p array)
         (write-n-bytes 1 1 stream)
         (write-n-bytes (fill-pointer array) +sequence-length+ stream))
        (t
         (write-n-bytes 0 2 stream)))
  (write-object (array-element-type array) stream)
  (write-n-bytes (boolify (adjustable-array-p array)) 1 stream)
  (loop for i below (array-total-size array)
        do (write-object (row-major-aref array i) stream)))

(defun read-array-fill-pointer (stream)
  (if (plusp (read-n-bytes 1 stream))
      (read-n-bytes +sequence-length+ stream)
      (not (read-n-bytes 1 stream))))

(defreader array (stream)
  (let ((array (make-array (read-next-object stream)
                           :fill-pointer (read-array-fill-pointer stream)
                           :element-type (read-next-object stream)
                           :adjustable (plusp (read-n-bytes 1 stream)))))
    (loop for i below (array-total-size array)
          do (setf (row-major-aref array i) (read-next-object stream)))
    array))

;;; Hash-table

(defvar *hash-table-tests* #(eql equal equalp eq))
(declaim (simple-vector *hash-table-tests*))

(defun check-hash-table-test (hash-table)
  (let* ((test (hash-table-test hash-table))
         (test-id (position test *hash-table-tests*)))
   (unless test-id
     (error "Only standard hashtable tests are supported, ~a has ~a"
            hash-table test))
    test-id))

(defmethod write-object ((hash-table hash-table) stream)
  (write-n-bytes #.(type-code 'hash-table) 1 stream)
  (write-n-bytes (check-hash-table-test hash-table) 1 stream)
  (write-n-bytes (hash-table-size hash-table) +hash-table-length+ stream)
  (loop for key being the hash-keys of hash-table
        using (hash-value value)
        do
        (write-object key stream)
        (write-object value stream))
  (write-n-bytes +end+ 1 stream))

(defreader hash-table (stream)
  (let* ((test (svref *hash-table-tests* (read-n-bytes 1 stream)))
         (size (read-n-bytes +hash-table-length+ stream))
         (table (make-hash-table :test test :size size)))
    (loop for id = (read-n-bytes 1 stream)
          until (eq id +end+)
          do (setf (gethash (call-reader id stream) table)
                   (read-next-object stream)))
    table))

;;; storable-class

(defstruct proxy
  class
  slot-locations
  id
  version)

(defun cache-class (class id)
  (when (<= (length *classes*) id)
    (adjust-array *classes* (1+ id)))
  (when (> (1+ id) (fill-pointer *classes*))
    (setf (fill-pointer *classes*) (1+ id)))
  (setf (aref *classes* id) class))

(defun make-class-proxy (proxy class &key id)
  (unless (class-finalized-p class)
    (finalize-inheritance class))
  (let ((new-id (if proxy
                    (proxy-id proxy)
                    (length *classes*)))
        (slot-locations (unless id
                          (map 'vector #'car
                               (slot-locations-and-initforms class))))
        (proxy (cond (id
                      (let ((intended (find-class-proxy class)))
                        (when (and intended
                                   (/= (proxy-id intended) id))
                          (cache-class intended id))
                        intended))
                     (proxy))))
    (cond (proxy
           (setf (proxy-slot-locations proxy) slot-locations
                 (proxy-version proxy) (version class)))
          (t
           (setf proxy (make-proxy :class class
                                   :id (or id new-id)
                                   :slot-locations slot-locations
                                   :version (version class)))
           (if id
               (cache-class proxy id)
               (vector-push-extend proxy *classes*))))
    proxy))

(defun find-class-proxy (class)
  (find class *classes* :test #'eq
                        :key (lambda (x)
                               (and (typep x 'proxy)
                                    (proxy-class x)))))

(defmethod write-object ((class storable-class) stream)
  (unless (class-finalized-p class)
    (finalize-inheritance class))
  (let ((proxy (find-class-proxy class)))
    (cond ((and proxy
                (= (version class)
                   (proxy-version proxy)))
           (proxy-id proxy))
          (t
           (let ((proxy (make-class-proxy proxy class))
                 (slots (slots-to-store class)))
             (write-n-bytes #.(type-code 'storable-class) 1 stream)
             (write-object (class-name class) stream)
             (write-n-bytes (proxy-id proxy) +class-id-length+ stream)
             (write-n-bytes (length slots) +sequence-length+ stream)
             (loop for slot across slots
                   do (write-object (slot-definition-name slot)
                                    stream))
             (proxy-id proxy))))))

(defun class-changed-after-read (proxy class)
  (let ((slot-locations-read (proxy-slot-locations proxy))
        (slot-locations-and-initforms (slot-locations-and-initforms class)))
    (or (null slot-locations-read)
        (/= (length slot-locations-read)
            (length slot-locations-and-initforms))
        (loop for read across slot-locations-read
              for (actual) across slot-locations-and-initforms
              thereis (not (eql read actual))))))

(defreader storable-class (stream)
  (let* ((class (find-class (read-next-object stream)))
         (proxy (make-class-proxy nil class
                                  :id (read-n-bytes +class-id-length+ stream))))
    (let* ((length (read-n-bytes +sequence-length+ stream))
           (vector (make-array length :initial-element nil)))
      (loop for i below length
            for slot-d =
            (slot-effective-definition class (read-next-object stream))
            when slot-d
            do (setf (aref vector i) (slot-definition-location slot-d)))
      (setf (proxy-slot-locations proxy) vector)
      (when (class-changed-after-read proxy class)
        (setf (proxy-version proxy) 0)))
    (read-next-object stream)))

;;; storable-object

(defun write-objects-inside-slots (object stream)
  (let ((visited (make-hash-table :test #'eq)))
    (labels ((write-changed (x)
               (cond ((not (typep x 'storable-object)))
                     ((gethash x visited))
                     ((written x)
                      (setf (gethash x visited) t)
                      (recurse x))
                     ((typep x 'storable-versioned-object)
                      (write-storable-versioned-object x stream))
                     (t
                      (write-storable-object x stream))))
             (recurse (object)
               (loop for (slot-loc) across (slot-locations-and-initforms (class-of object))
                     for value = (standard-instance-access object slot-loc)
                     do
                     (cond ((consp value)
                            (loop for (x) on value
                                  do (write-changed x)))
                           ((and (vectorp value)
                                 (not (stringp value)))
                            (loop for x across value
                                  do (write-changed x)))
                           (t (write-changed value))))))
      (recurse object))))

(defmethod write-object ((object storable-object) stream)
  (cond ((is-written object)
         (let* ((class (class-of object))
                (class-id (write-object class stream)))
           (write-n-bytes #.(type-code 'storable-link) 1 stream)
           (write-n-bytes class-id +class-id-length+ stream)
           (write-n-bytes (assign-id object) +id-length+ stream)))
        ((typep object 'storable-versioned-object)
         (write-storable-versioned-object object stream))
        (t
         (write-storable-object object stream))))

(defun get-class (id)
  (aref *classes* id))

(declaim (inline get-instance))
(defun get-instance (class-id id)
  (let* ((proxy (get-class class-id))
         (class (if (typep proxy 'proxy)
                    (proxy-class proxy)
                    proxy))
         (index (if (typep class 'storable-class)
                    (id-cache class)
                    *indexes*))
         (existing (gethash id index)))
    (values (or existing
                (setf (gethash id index)
                      (fast-allocate-instance class)))
            id
            (and existing
                 (typep existing 'storable-object)
                 (id existing)
                 t)
            class-id)))

(defun read-instance (stream)
  (get-instance (read-n-bytes +class-id-length+ stream)
                (read-n-bytes +id-length+ stream)))

(defreader storable-link (stream)
  (read-instance stream))

;;; storable-object

(defun assign-id (object)
  (cond (*export*
         (get-object-id object))
        ((id object))
        (t
         (setf (id object)
               (1- (incf (last-id (class-of object))))))))

(defun write-old-versions (object stream)
  (let ((old-versions (reverse (old-versions object))))
    (loop for old in old-versions
          do (write-storable-object old stream))))

(declaim (inline write-storable-object-common))
(defun write-storable-object-common (object stream &key before)
  (let* ((class (class-of object))
         (slots (slot-locations-and-initforms class))
         (class-id (write-object class stream))
         (id (assign-id object)))
    (declare (simple-vector slots))
    (setf (collection object) *collection*)
    (write-n-bytes #.(type-code 'storable-object) 1 stream)
    (write-n-bytes class-id +class-id-length+ stream)
    (write-n-bytes id +id-length+ stream)
    (when before
      (funcall before stream))
    (when *export*
      (write-object (if (or (eq (collection object) *collection*)
                            (not (top-level object)))
                        nil
                        (name (collection object)))
                    stream))
    (set-written object)
    (loop for id below (length slots)
          for (location . initform) = (aref slots id)
          for value = (standard-instance-access object location)
          unless (eql value initform)
          do
          (write-n-bytes id 1 stream)
          (if (eq value 'sb-pcl::..slot-unbound..)
              (write-n-bytes +unbound-slot+ 1 stream)
              (write-object value stream)))
    (write-n-bytes +end+ 1 stream)))

(defun write-storable-object (object stream)
  (write-storable-object-common object stream))

(defun write-storable-versioned-object (object stream)
  (let ((previous (car (old-versions object))))
    (when (and previous
               (not (effective-date previous)))
      (setf (effective-date previous)
            (stamp-date object)))
    (when *snapshot*
      (write-old-versions object stream))
    (write-storable-object-common object stream
                                  :before
                                  (when *export*
                                    (lambda (stream)
                                      (write-object (old-versions object)
                                                    stream))))))

(defun set-id (class object id)
  (setf (id object) id)
  (when (>= id (last-id class))
    (setf (last-id class) (1+ id))))


(defun clear-previous-version (object)
  (loop for (location . initform) across
        (slot-locations-and-initforms (class-of object))
        do (setf (standard-instance-access object location)
                 initform)))

(defun read-slots (object slots stream)
  (declare (simple-vector slots))
  (loop for slot-id = (read-n-bytes 1 stream)
        until (= slot-id +end+)
        do
        (let* ((location (aref slots slot-id))
               (code (read-n-bytes 1 stream))
               (value (if (= code +unbound-slot+)
                          'sb-pcl::..slot-unbound..
                          (call-reader code stream))))
          (when location
            (setf (standard-instance-access object location)
                  value)))))

(defun assign-read-id (object id)
  (cond ((id object))
        (*import*
         (assign-id object))
        (t
         (set-id (class-of object) object id)
         (setf (written object) t))))

(defvar *do-not-push-into-collection* nil)

(defreader storable-object (stream)
  (multiple-value-bind (object id existing class-id) (read-instance stream)
    ;; To work with the old db files, this supports versioning as well
    (let* ((versioned-p (typep object 'storable-versioned-object) )
           (old-versions
             (when (and *import* versioned-p)
               (let ((*do-not-push-into-collection* t))
                 (read-next-object stream))))
           (proxy (get-class class-id))
           (slots (proxy-slot-locations proxy))
           (copy (cond ((not existing)
                        nil)
                       (versioned-p
                        (prog1 (copy-object object)
                          (clear-previous-version object)))))
           (collection-name (and *import*
                                 (read-next-object stream)))
           (*collection* (if collection-name
                             (find-collection *db* collection-name)
                             *collection*)))
      (assign-read-id object id)
      (read-slots object slots stream)
      (when *import*
        (setf (written object) nil)
        (when versioned-p
          (setf (old-versions object) old-versions)))
      (setf (collection object) *collection*)
      (cond ((and copy versioned-p)
             (supersede object copy :set-time t)
             (unless (eq (collection object)
                         (collection copy))
               (warn "Collection mismatch for ~a: ~a => ~a"
                     object
                     (and (collection copy) (name (collection copy)))
                     (and (collection object) (name (collection object))))))
            ((or (not (top-level object))
                 *do-not-push-into-collection*))
            ((and *import*
                  (let ((existing (find-existing-doc object *collection*)))
                    (and existing
                         (setf object existing)))))
            (t
             (vector-push-extend object (docs *collection*))
             (when *import*
               (serialize-doc *collection*
                              object)))))
    object))

;;; standard-class

(defmethod write-object ((class standard-class) stream)
  (cond ((position class *classes* :test #'eq))
        (t
         (unless (class-finalized-p class)
           (finalize-inheritance class))
         (let ((id (vector-push-extend class *classes*))
               (slots (class-slots class)))
           (write-n-bytes #.(type-code 'standard-class) 1 stream)
           (write-object (class-name class) stream)
           (write-n-bytes id +class-id-length+ stream)
           (write-n-bytes (length slots) +sequence-length+ stream)
           (loop for slot in slots
                 do (write-object (slot-definition-name slot)
                                  stream))
           id))))

(defreader standard-class (stream)
  (let ((class (find-class (read-next-object stream))))
    (cache-class class
                 (read-n-bytes +class-id-length+ stream))
    (unless (class-finalized-p class)
      (finalize-inheritance class))
    (let ((length (read-n-bytes +sequence-length+ stream)))
      (loop for i below length
            do (slot-effective-definition class (read-next-object stream))))
    (read-next-object stream)))

;;; standard-link

(defun write-standard-link (object stream)
  (let* ((class (class-of object))
         (class-id (write-object class stream)))
    (write-n-bytes #.(type-code 'standard-link) 1 stream)
    (write-n-bytes class-id +class-id-length+ stream)
    (write-n-bytes (get-object-id object) +id-length+ stream)))

(defreader standard-link (stream)
  (read-instance stream))

;;; standard-object

(defun get-object-id (object)
  (or (gethash object *object-cache*)
      (setf (gethash object *object-cache*)
            (if *export*
                (1- (incf *export-last-id*))
                (1- (incf (last-id *collection*)))))))

(defmethod write-object ((object standard-object) stream)
  (if (gethash object *written-objects*)
      (write-standard-link object stream)
      (let* ((class (class-of object))
             (slots (class-slots class))
             (class-id (write-object class stream)))
        (write-n-bytes #.(type-code 'standard-object) 1 stream)
        (write-n-bytes class-id +class-id-length+ stream)
        (write-n-bytes (get-object-id object) +id-length+ stream)
        (setf (gethash object *written-objects*) t)
        (loop for id from 0
              for slot in slots
              for location = (slot-definition-location slot)
              for initform = (slot-definition-initform slot)
              for value = (standard-instance-access object location)
              do
              (write-n-bytes id 1 stream)
              (if (eq value 'sb-pcl::..slot-unbound..)
                  (write-n-bytes +unbound-slot+ 1 stream)
                  (write-object value stream)))
        (write-n-bytes +end+ 1 stream))))

(defreader standard-object (stream)
  (multiple-value-bind (instance id) (read-instance stream)
   (let* ((class (class-of instance))
          (slots (class-slots class)))
     (when (and (not *import*)
                (>= id (last-id *collection*)))
       (setf (last-id *collection*) (1+ id)))
     (flet ((read-slot ()
              (let ((code (read-n-bytes 1 stream)))
                (if (= code +unbound-slot+)
                    'sb-pcl::..slot-unbound..
                    (call-reader code stream)))))
       (loop for slot-id = (read-n-bytes 1 stream)
             until (= slot-id +end+)
             do
             (let ((slot (nth slot-id slots)))
               (if slot
                   (setf (standard-instance-access instance
                                                   (slot-definition-location slot))
                         (read-slot))
                   (read-slot)))))
     instance)))

;;; collection

(defmethod write-object ((collection collection) stream)
  (write-n-bytes #.(type-code 'collection) 1 stream)
  (when *export*
    (write-object (name collection) stream)))

(defreader collection (stream)
  (if *import*
      (find-collection *db*
                      (read-next-object stream))
      *collection*))

;;; delete

(defun write-delete-marker (object stream)
  (when (id object)
    (let* ((class (class-of object))
           (class-id (write-object class stream)))
      (write-n-bytes #.(type-code 'delete-marker) 1 stream)
      (write-n-bytes class-id +class-id-length+ stream)
      (write-n-bytes (id object) +id-length+ stream))))

(defreader delete-marker (stream)
  (alexandria:deletef (docs *collection*) (read-instance stream)))

;;; clear version

(defun write-clear-version-marker (object stream)
  (let* ((class (class-of object))
         (class-id (write-object class stream)))
    (write-n-bytes #.(type-code 'clear-version-marker) 1 stream)
    (write-n-bytes class-id +class-id-length+ stream)
    (write-n-bytes (id object) +id-length+ stream)))

(defreader clear-version-marker (stream)
  (let* ((class-id (read-n-bytes +class-id-length+ stream))
         (id (read-n-bytes +id-length+ stream))
         (object (get-instance class-id id)))
    (setf (old-versions object) nil)))

;;;
#+sbcl (declaim (inline %fast-allocate-instance))

#+sbcl
(defun %fast-allocate-instance (wrapper initforms)
  (declare (simple-vector initforms))
  (let ((instance (sb-pcl::%make-standard-instance
                   (copy-seq initforms) (sb-pcl::get-instance-hash-code))))
    (setf (sb-pcl::std-instance-wrapper instance)
          wrapper)
    instance))

#+sbcl
(defun fast-allocate-instance (class)
  (declare (optimize speed))
  (if (typep class 'storable-class)
      (let ((initforms (class-initforms class))
            (wrapper (sb-pcl::class-wrapper class)))
        (%fast-allocate-instance wrapper initforms))
      (allocate-instance class)))

(defun clear-cache (collection)
  (setf (classes collection) (make-class-cache)
        (packages collection) (make-s-packages)))

;;;

(defmacro with-collection-lock (collection &body body)
  `(bt:with-lock-held ((lock ,collection))
     ,@body))

(defun load-data (collection file)
  (with-collection-lock collection
    (when (loaded collection)
      (warn "Collection ~a cannot be loaded twice." collection))
    (with-collection collection
      (with-io-file (stream file)
        (loop until (stream-end-of-file-p stream)
              do (read-next-object stream))))))

(defun save-data (collection file)
  (with-collection-lock collection
    (let ((*written-objects* (make-hash-table :test 'eq))
          (*snapshot* t))
      (clear-cache collection)
      (with-collection collection
        (with-io-file (stream file
                       :direction :output)
          (dump-data stream)))
      (values))))

(defun save-doc (collection document file)
  (with-collection-lock collection
    (let ((*written-objects* (make-hash-table :test 'eq)))
      (with-collection collection
        (with-io-file (stream file
                       :direction :output
                       :append t)
          (write-top-level-object document stream))))))

(defun %delete-doc (collection document file)
  (with-collection-lock collection
    (with-collection collection
      (with-io-file (stream file
                     :direction :output
                     :append t)
        (write-delete-marker document stream)))))

(defun %clear-versions (collection document file)
  (with-collection-lock collection
    (with-collection collection
      (with-io-file (stream file
                     :direction :output
                     :append t)
        (write-clear-version-marker document stream)))))

;;; Export/import

(defun export-db (db file test)
  (let ((*inhibit-change-marking* t)
        (*written-objects* (make-hash-table :test 'eq))
        (*packages* (make-s-packages))
        (*classes* (make-class-cache))
        (*indexes* (make-hash-table :size 1000))
        (*object-cache* (make-hash-table :size 1000
                                         :test #'eq))
        (*export* t)
        (*export-last-id* 0))
    (with-io-file (stream file
                   :direction :output)
      (loop for *collection* being the hash-value
            of (collections db)
            do
            (write-object (name *collection*) stream)
            (loop for doc across (docs *collection*)
                  when (funcall test doc)
                  do (write-top-level-object doc stream))
            (write-n-bytes +end+ 1 stream)))
    (values)))

(defun import-db (db file)
  (let ((*inhibit-change-marking* t)
        (*written-objects* (make-hash-table :test 'eq))
        (*packages* (make-s-packages))
        (*classes* (make-class-cache))
        (*indexes* (make-hash-table :size 1000))
        (*object-cache* (make-hash-table :size 1000
                                         :test #'eq))
        (*import* t)
        (*db* db))
    (with-io-file (stream file)
      (loop until (stream-end-of-file-p stream)
            for *collection* = (find-collection db
                                                (read-next-object stream))
            do
            (loop for code = (read-n-bytes 1 stream)
                  until (= code +end+)
                  do
                  (call-reader code stream))))
    (values)))
