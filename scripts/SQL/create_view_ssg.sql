
--  RECREATE ALL VIEWS – CLEAN & SAFE VERSION


SET NOCOUNT ON;
GO

-- 1. StyleBasicinformation
IF OBJECT_ID('dbo.StyleBasicinformation', 'V') IS NOT NULL
    DROP VIEW dbo.StyleBasicinformation;
GO

CREATE VIEW dbo.StyleBasicinformation AS
SELECT 
    id,
    FG_ArticleNo,
    CAST(Model AS varchar(50)) AS Model,
    HSCode_ID,
    RMVendor_ID,
    CreationDate,
    FG_Season,
    PicEntryNo,
    Category_ID
FROM dbo.Coa31
WHERE FMaterial = 1
  AND ItemActive = 1
  AND CreationDate > '2024-06-30'        -- Fixed: proper date format
  AND FG_Season = '112';
GO


-- 2. Operationinformation
IF OBJECT_ID('dbo.Operationinformation', 'V') IS NOT NULL
    DROP VIEW dbo.Operationinformation;
GO

CREATE VIEW dbo.Operationinformation AS
SELECT 
    a.MainDeptt_ID,
    a.ApplicableDate,
    a.ArticleNo,
    a.ConversionFactor,
    a.Mnth,
    a.vno,
    a.Dated,
    b.SubOperation_ID,
    b.SMV,
    b.Machine
FROM dbo.OperationBreakDown a
INNER JOIN dbo.OperationBreakDown_det b 
    ON a.Mnth  = b.Mnth 
   AND a.vYear = b.vYear 
   AND a.vno   = b.vno
WHERE a.Dated > '2025-06-30'              -- Fixed date
  AND a.MainDeptt_ID = '002'
  AND b.vYear = 25
  AND CAST(b.Mnth AS int) > 6;           -- Safer than string comparison
GO


-- 3. Loadinginformation
IF OBJECT_ID('dbo.Loadinginformation', 'V') IS NOT NULL
    DROP VIEW dbo.Loadinginformation;
GO

CREATE VIEW dbo.Loadinginformation AS
SELECT 
    Dated,
    BarCode,
    MainDeptt_ID,
    PONo,
    Item_ID,
    BundleNo,
    qty,
    Line_ID
FROM dbo.ApparalProduction
WHERE MainDeptt_ID = 'DTS'
  AND Dated > '2025-09-30';               -- Fixed date format
GO


-- 4. INAEmployees
IF OBJECT_ID('dbo.INAEmployees', 'V') IS NOT NULL
    DROP VIEW dbo.INAEmployees;
GO

CREATE VIEW dbo.INAEmployees AS
SELECT 
    ID,
    Title,
    Desig_ID,
    Deptt_ID,
    Line_ID,
    Location_ID,
    Joindate,
    add1,
    Mobile,
    ActiveStatus,
    Male
FROM dbo.EmployeeDetails
WHERE Deptt_ID = '1-06-13'
  AND Location_ID BETWEEN '040' AND '041'
  AND ActiveStatus = 'active';
GO


-- 5. hangerline_emp (MOST IMPORTANT ONE – FULLY FIXED)
IF OBJECT_ID('dbo.hangerline_emp', 'V') IS NOT NULL
    DROP VIEW dbo.hangerline_emp;
GO

CREATE VIEW dbo.hangerline_emp AS
SELECT
    e.ID,
    REPLACE(e.ID, '-', '') AS INA_ID,
    e.Title,
		e.FatherName,
    e.Desig_ID,
    e.Deptt_ID,
    e.Line_ID                     AS Current_Line_ID,
    COALESCE(c.Line_ID, e.Line_ID) AS Latest_Line_ID,
    COALESCE(c.EffectiveDate, e.Joindate) AS Assignment_Date,

    CASE COALESCE(c.Line_ID, e.Line_ID)
        WHEN '021' THEN 'line-21' WHEN '21N' THEN 'line-21' WHEN '21' THEN 'line-21'
        WHEN '022' THEN 'line-22' WHEN '22N' THEN 'line-22' WHEN '22' THEN 'line-22'
        WHEN '023' THEN 'line-23' WHEN '23N' THEN 'line-23' WHEN '23' THEN 'line-23'
        WHEN '024' THEN 'line-24' WHEN '24N' THEN 'line-24' WHEN '24' THEN 'line-24'
        WHEN '025' THEN 'line-25' WHEN '25N' THEN 'line-25' WHEN '25' THEN 'line-25'
        WHEN '026' THEN 'line-26' WHEN '26N' THEN 'line-26' WHEN '26' THEN 'line-26'
        WHEN '027' THEN 'line-27' WHEN '27N' THEN 'line-27' WHEN '27' THEN 'line-27'
        WHEN '028' THEN 'line-28' WHEN '28N' THEN 'line-28' WHEN '28' THEN 'line-28'
        WHEN '029' THEN 'line-29' WHEN '29N' THEN 'line-29' WHEN '29' THEN 'line-29'
        WHEN '030' THEN 'line-30' WHEN '30N' THEN 'line-30' WHEN '30' THEN 'line-30'
        WHEN '031' THEN 'line-31' WHEN '31N' THEN 'line-31' WHEN '31' THEN 'line-31'
        WHEN '032' THEN 'line-32' WHEN '32N' THEN 'line-32' WHEN '32' THEN 'line-32'
        WHEN '033' THEN 'line-33' WHEN '33N' THEN 'line-33' WHEN '33' THEN 'line-33'
        ELSE 'Other Line'
    END AS Line_Desc,

    CASE WHEN CHARINDEX('N', COALESCE(c.Line_ID, e.Line_ID)) > 0 THEN 'Night' ELSE 'Day' END AS Shift,

    e.Location_ID,
    e.Joindate,
		e.ResignDate,
		e.NICNew as NIC,
    e.add1,
    e.Mobile,
    case when e.ActiveStatus='Active' then 0 else 1 end as ActiveStatus,
    case when e.Male=1 then 'M' else 'F' end as gender

FROM dbo.EmployeeDetails e
LEFT JOIN (
    SELECT 
        Emp_ID,
        Line_ID,
        EffectiveDate
    FROM (
        SELECT 
            Emp_ID,
            Line_ID,
            EffectiveDate,
            ROW_NUMBER() OVER (PARTITION BY Emp_ID ORDER BY EffectiveDate DESC) AS rn
        FROM dbo.ChangeEmpLine
        WHERE EffectiveDate IS NOT NULL
    ) x
    WHERE rn = 1
) c ON c.Emp_ID = e.ID

WHERE e.Deptt_ID = '1-06-13'
--   AND CAST(e.modified_at AS DATE) = CAST(GETDATE() AS DATE) -- Fixed: proper date comparison for today
    ;
  
GO

PRINT 'All 5 views recreated successfully!';