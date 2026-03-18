--- cleaning data ---
select *
from healthcare_dataset;

--- creating staging table ---
CREATE TABLE `healthcare_dataset_01` (
  `name` text,
  `age` int DEFAULT NULL,
  `gender` text,
  `blood_type` text,
  `medical_condition` text,
  `date_of_admission` text,
  `doctor` text,
  `hospital` text,
  `insurance_provider` text,
  `billing_amount` double DEFAULT NULL,
  `room_number` int DEFAULT NULL,
  `admission_type` text,
  `discharge_date` text,
  `medication` text,
  `test_results` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select
*
from healthcare_dataset_01;

--- insert data ---
insert healthcare_dataset_01
select *
from healthcare_dataset;

--- cleaning duplicates ---
select
*,
row_number() over(partition by `name`,age,gender,blood_type,medical_condition,date_of_admission,doctor,hospital,insurance_provider,billing_amount,room_number,admission_type,discharge_date,medication,test_results) as row_num
from healthcare_dataset_01
;

with duplicate_cte as
(select
*,
row_number() over(partition by `name`,age,gender,blood_type,medical_condition,date_of_admission,doctor,hospital,insurance_provider,billing_amount,room_number,admission_type,discharge_date,medication,test_results) as row_num
from healthcare_dataset_01
)
select
*
from duplicate_cte
where row_num > 1;

CREATE TABLE `healthcare_dataset_02` (
  `name` text,
  `age` int DEFAULT NULL,
  `gender` text,
  `blood_type` text,
  `medical_condition` text,
  `date_of_admission` text,
  `doctor` text,
  `hospital` text,
  `insurance_provider` text,
  `billing_amount` double DEFAULT NULL,
  `room_number` int DEFAULT NULL,
  `admission_type` text,
  `discharge_date` text,
  `medication` text,
  `test_results` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into healthcare_dataset_02
select
*,
row_number() over(partition by `name`,age,gender,blood_type,medical_condition,date_of_admission,doctor,hospital,insurance_provider,billing_amount,room_number,admission_type,discharge_date,medication,test_results) as row_num
from healthcare_dataset_01
;

select *
from healthcare_dataset_02;

delete from healthcare_dataset_02
where row_num > 1;

--- standarize columns ---
select
`name`,
CONCAT(
  UPPER(SUBSTRING(`name`, 1, 1)),
  (SUBSTRING(`name`, 2))
)
from healthcare_dataset_02;

update healthcare_dataset_02
set `name` = upper(`name`)
;

select *
from healthcare_dataset_02;
--- checking for date schemas ---
select 
date_of_admission,
str_to_date(date_of_admission, '%m/%m/%Y'),
discharge_date,
str_to_date(discharge_date,'%m/%d/%Y')
from healthcare_dataset_02;

--- rounding off billing_amount ---
select 
billing_amount,
round(billing_amount,2)
from healthcare_dataset_02;

update healthcare_dataset_02
set billing_amount = round(billing_amount,2);

--- Converting negative billings ---
select
billing_amount,
abs(billing_amount)
from healthcare_dataset_02
order by billing_amount asc;

update healthcare_dataset_02
set billing_amount = abs(billing_amount);

select *
from healthcare_dataset_02;

--- delete row_num column ---
alter table healthcare_dataset_02
drop column row_num;

select *
from
healthcare_dataset_02;


