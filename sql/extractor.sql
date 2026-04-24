WITH doctor_info AS (
    SELECT
        d.doctor_id,
        d.first_name,
        d.last_name,
        d.specialization,
        d2.dept_name,
        CASE
            WHEN ('Dr. ' || TRIM(d.first_name) || ' ' || TRIM(d.last_name))
                 IN (SELECT head_doctor FROM {INDUSTRY}.departments)
            THEN 'YES'
            ELSE 'NO'
        END AS is_head_doctor
    FROM {INDUSTRY}.doctors d
    INNER JOIN {INDUSTRY}.departments d2
        ON d.dept_id = d2.dept_id
),

patient_appt AS (
    SELECT
        a.appointment_date,
        a.appointment_id,
        a.doctor_id,
        p.patient_id,
        a.status             AS appointment_status,
        a.visit_type,
        COALESCE(a.fee, 0)   AS fee,          
        p.insurance_type,
        TRIM(p.first_name) || ' ' || TRIM(p.last_name) AS patient_name
    FROM {INDUSTRY}.appointments a
    LEFT JOIN {INDUSTRY}.patients p
        ON a.patient_id = p.patient_id
)

SELECT
    p.patient_name,
    p.insurance_type,
    d.dept_name,
    d.specialization,
    'Dr. ' || TRIM(d.first_name) || ' ' || TRIM(d.last_name) AS doctor_name,
    d.is_head_doctor,
    b.payment_status,
    b.payment_method,

    -- appointment counts
    COUNT(*)                                                        AS total_appointments,
    COUNT(CASE WHEN p.appointment_status = 'Completed' THEN 1 END) AS completed,
    COUNT(CASE WHEN p.appointment_status = 'Scheduled' THEN 1 END) AS scheduled,
    COUNT(CASE WHEN p.appointment_status = 'Cancelled' THEN 1 END) AS cancelled,
    COUNT(CASE WHEN p.appointment_status = 'No-Show'   THEN 1 END) AS no_shows,


    -- billing summary
    SUM(COALESCE(b.amount_charged, 0))  AS total_charged,       
    SUM(COALESCE(b.insurance_paid, 0))  AS total_insurance_paid,
    SUM(COALESCE(b.patient_paid,  0))   AS total_patient_paid

FROM patient_appt p                                              
LEFT JOIN {INDUSTRY}.billing b  ON b.appointment_id = p.appointment_id
LEFT JOIN doctor_info d         ON p.doctor_id      = d.doctor_id

GROUP BY
    p.patient_name,
    p.insurance_type,
    d.dept_name,
    d.specialization,
    'Dr. ' || TRIM(d.first_name) || ' ' || TRIM(d.last_name),
    d.is_head_doctor,
    b.payment_status,
    b.payment_method

