;;; -*- Mode: Lisp -*-

;;; This software is in the public domain and is
;;; provided with absolutely no warranty.

(in-package #:xdb2)

(defclass storable-class (standard-class)
  ((class-id :initform nil
             :accessor class-id)
   (slots-to-store :initform nil
                   :accessor slots-to-store)
   (slot-locations-and-initforms
    :initform nil
    :accessor slot-locations-and-initforms)
   (all-slot-locations-and-initforms
    :initform nil
    :accessor all-slot-locations-and-initforms)
   (initforms :initform #()
	      :accessor class-initforms)
   (last-id :initform 0
            :accessor last-id)
   (id-cache :initform (make-hash-table :size 1000)
             :accessor id-cache)
   (version :initarg :version
            :initform 0
            :accessor version)
   (key-comparer :initform nil
                 :accessor key-comparer)))

(defclass storable-versioned-class (storable-class)
  ())

(defun initialize-storable-class (next-method class default-superclass
                                  &rest args
                                  &key name direct-superclasses &allow-other-keys)
  (if (equal name default-superclass)
      (funcall next-method)
      (let ((default-superclass (find-class default-superclass)))
        (if (some (lambda (x) (subtypep x default-superclass))
                  direct-superclasses)
            (funcall next-method)
            (apply next-method class
                   :direct-superclasses (list* default-superclass direct-superclasses)
                   args)))))

(defmethod initialize-instance :around ((class storable-class)
                                        &rest args)
  (apply #'initialize-storable-class #'call-next-method
         class 'storable-object args))

(defmethod reinitialize-instance :around ((class storable-class)
                                          &rest args)
  (apply #'initialize-storable-class #'call-next-method
         class 'storable-object args))

(defmethod initialize-instance :around ((class storable-versioned-class)
                                        &rest args)
  (apply #'initialize-storable-class #'call-next-method
         class 'storable-versioned-object args))

(defmethod reinitialize-instance :around ((class storable-versioned-class)
                                          &rest args)
  (apply #'initialize-storable-class #'call-next-method
         class 'storable-versioned-object args))

;;;

(defmethod validate-superclass
    ((class standard-class)
     (superclass storable-class))
  nil)

(defmethod validate-superclass
    ((class storable-class)
     (superclass standard-class))
  t)

(defclass storable-slot-mixin ()
  ((storep :initarg :storep
           :initform t
           :accessor store-slot-p)
   (key :initarg :key
        :initform nil
        :accessor key)))

(defclass storable-direct-slot-definition (storable-slot-mixin
                                           standard-direct-slot-definition)
  ())

(defclass storable-effective-slot-definition
    (storable-slot-mixin standard-effective-slot-definition)
  ())

(defmethod direct-slot-definition-class ((class storable-class)
                                         &rest initargs)
  (declare (ignore initargs))
  (find-class 'storable-direct-slot-definition))

(defmethod effective-slot-definition-class ((class storable-class)
                                            &key &allow-other-keys)
  (find-class 'storable-effective-slot-definition))

(defmethod compute-effective-slot-definition
    ((class storable-class) slot-name direct-definitions)
  (declare (ignore slot-name))
  (let ((effective-definition (call-next-method))
        (direct-definition (car direct-definitions)))
    (when (typep direct-definition 'storable-direct-slot-definition)
      (setf (store-slot-p effective-definition)
          (store-slot-p direct-definition)
          (key effective-definition)
          (key direct-definition)))
    effective-definition))

(defun make-slots-cache (slot-definitions)
  (map 'vector
       (lambda (slot-definition)
	 (cons (slot-definition-location slot-definition)
	       (slot-definition-initform slot-definition)))
       slot-definitions))

(defun key-slots (class)
  (remove-if-not #'key (class-slots class)))

(defun make-key-comparer (class)
  (let ((slots (key-slots class)))
    (and
     slots
     (compile
      nil
      `(lambda (x y)
         (let ((class (class-of x)))
           (and
            (eq class (class-of y))
            ,@(loop for slot in slots
                    for location = (slot-definition-location slot)
                    append
                    `((slot-boundp-using-class class x ,slot)
                      (slot-boundp-using-class class y ,slot)
                      (equal (standard-instance-access x ,location)
                             (standard-instance-access y ,location)))))))))))

(defun initialize-class-slots (class slots)
  (let* ((slots-to-store (coerce (remove-if-not #'store-slot-p slots)
                                 'simple-vector)))
    (setf (slots-to-store class)
          slots-to-store)
    (setf (slot-locations-and-initforms class)
          (make-slots-cache slots-to-store))
    (setf (all-slot-locations-and-initforms class)
          (make-slots-cache slots))
    (setf (class-initforms class)
          (map 'vector #'slot-definition-initform slots))
    (incf (version class))
    (setf (key-comparer class) (make-key-comparer class))))

(defmethod finalize-inheritance :after ((class storable-class))
  (initialize-class-slots class (class-slots class)))

;;;

(defclass storable-object (standard-object)
  ((id :initform nil
       :accessor id
       :storep nil)
   (written :initform nil
            :accessor written
            :storep nil)
   (collection :initarg :collection
               :initform nil
               :accessor collection
               :storep nil)
   (top-level :initarg :top-level
              :initform nil
              :accessor top-level))
  (:metaclass storable-class))

;;;

(defclass storable-versioned-object (storable-object)
  ((old-versions :initarg :old-versions
                 :initform nil
                 :accessor old-versions
                 :storep nil)
   (stamp-date :initarg :stamp-date
               :initform nil
               :accessor stamp-date)
   (effective-date :initarg :effective-date
                   :initform nil
                   :accessor effective-date))
  (:metaclass storable-versioned-class))

(defmethod version ((object storable-versioned-object))
  (1+ (length (old-versions object))))

(defvar *inhibit-change-marking* nil)

(defgeneric supersede (object old-object &key)
  (:method ((object t) (old-object t) &key)))

(defmethod supersede ((object storable-versioned-object) old-object
                      &key set-time)
  (let ((*inhibit-change-marking* t))
    (when set-time
      (setf (effective-date old-object)
            (stamp-date object)))
    (push old-object (old-versions object))
    (setf (old-versions old-object) nil)))

(defmethod (setf slot-value-using-class)
    (new-value (class storable-versioned-class) object slotd)
  (when (and (not *inhibit-change-marking*)
             (slot-boundp-using-class class object slotd)
             (written object))
    (let ((current-value (slot-value-using-class class object slotd))
          (*inhibit-change-marking* t))
      (unless (equal new-value current-value)
        (setf (written object) nil)
        (supersede object (copy-object object)))))
  (call-next-method))
