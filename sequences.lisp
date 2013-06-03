(in-package #:xdb2)

(defclass xdb-sequence ()
  ((key :initarg :key
         :accessor key)
   (value :initarg :value
          :accessor value))
  (:metaclass storable-class))

(defgeneric enable-sequences (xdb &key collection-class))

(defmethod enable-sequences ((xdb xdb) &key collection-class)
  (add-collection xdb "sequences" 
                  :collection-class (or collection-class 'collection)))

(defgeneric next-sequence (xdb key sequence-class))

(defun get-sequence-max (collection value)
  (let ((max 0)
        (return-doc))
    (map-docs
     nil
     (lambda (doc)
       (when (equal (get-val doc 'key) value)
         (when (> (get-val doc 'value) max)
             (setf max (get-val doc 'value))
             (setf return-doc doc))))
     collection)
    return-doc))

(defmethod next-sequence ((xdb xdb) key sequence-class)
  (let* ((sequences (get-collection xdb "sequences"))
         (doc (get-sequence-max sequences  key)))
    (unless doc
      (setf doc (make-instance sequence-class  :key key :value 0)))
    (incf (get-val doc 'value))
    (persist doc)
    (get-val doc 'value)))