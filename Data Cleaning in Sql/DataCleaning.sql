-- Data Cleaning


select *
from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns



Create Table layoffs_staging
Like layoffs;


Select *
From layoffs_staging;

Insert layoffs_staging
Select *
From layoffs;


Select *,
Row_number() Over(
Partition by company, industry, total_laid_off, percentage_laid_off, `date`) As row_num
From layoffs_staging;

With duplicate_cte as
(
Select *,
Row_number() Over(
Partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) As row_num
From layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


Select *
From layoffs_staging
where company = '#Paid';


With duplicate_cte as
(
Select *,
Row_number() Over(
Partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) As row_num
From layoffs_staging
)
Delete
from duplicate_cte
where row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
Select *,
Row_number() Over(
Partition by company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) As row_num
From layoffs_staging;

set sql_safe_updates = 0;

Delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- Standardizing Data (find issue &fix)

select Company, TRIM(company)
from layoffs_staging2;

 Update layoffs_staging2
 set  Company = TRIM(company);
 
 
select DISTINCT industry 
from layoffs_staging2
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

select `date`
from layoffs_staging2;

Update layoffs_staging2
set `date`= str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2
where industry is null 
or industry = '';


select *
from layoffs_staging2
where company = 'Airbnb';


update layoffs_staging2 
set industry = 'travel'
where industry is null or industry = '';


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

Delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

Alter Table layoffs_staging2
Drop column row_num;






















