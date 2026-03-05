--- Cleaning Data ---

select
*
from
layoffs;

--- Creating Staging tables ---
create table layoffs_stage_01
like layoffs;

select *
from layoffs_stage_01;

--- Inserting Data ---
Insert layoffs_stage_01
select *
from layoffs;


--- Finding duplicates/Creating row_num for groupings ---
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_stage_01
;

-- Create CTE to call-out duplicates --

with duplicate_cte as
(select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_stage_01
)
select *
from duplicate_cte
where row_num >1;


--- Creating duplicate table staging ---
CREATE TABLE `layoffs_stage_02` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--- Insert data with row_num ---
insert into layoffs_stage_02
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_stage_01;

-- Confirming duplicate datas --
select *
from layoffs_stage_02
where company = 'Casper';

--- Delete duplicates ---
delete
from layoffs_stage_02
where row_num > 1;

select * 
from layoffs_stage_02;

--- Standardizing data ---
	-- checking each column --
select
company,
(trim(company))
from layoffs_stage_02;

update layoffs_stage_02
set company = trim(company);

select location
from layoffs_stage_02
group by location
order by 1;

select industry
from layoffs_stage_02
group by industry
order by 1;

select industry
from layoffs_stage_02
where industry like 'crypto%';

update layoffs_stage_02
set industry = 'Crypto'
where industry like 'crypto%';

select distinct industry
from layoffs_stage_02;


select distinct country
from layoffs_stage_02
order by 1;

update layoffs_stage_02
set country = 'United States'
where country like 'United States%';

select distinct country
from layoffs_stage_02
order by 1;

select *
from layoffs_stage_02;

--- Standardize Date ---
select `date`
from layoffs_stage_02;
-- Check for schema --
-- String to date --
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_stage_02;

update layoffs_stage_02
set `date` = str_to_date(`date`,'%m/%d/%Y');

select *
from layoffs_stage_02;

alter table layoffs_stage_02
modify column `date` date;

--- Checking for null and blanks ---
select *
from layoffs_stage_02
where industry is null
or industry = '';

select *
from layoffs_stage_02
where company = 'Airbnb';

update layoffs_stage_02
set industry = 'Travel'
where company = 'Airbnb';

select *
from layoffs_stage_02
where company like 'Bally%';

select *
from layoffs_stage_02
where company = 'Carvana';

update layoffs_stage_02
set industry = 'Transportation'
where company = 'Carvana';

select *
from layoffs_stage_02
where company = 'Juul';

update layoffs_stage_02
set industry = 'Consumer'
where company = 'Juul';

select *
from layoffs_stage_02;

--- Delete row_num column ---
Alter table layoffs_stage_02
drop column row_num;

select *
from layoffs_stage_02;
