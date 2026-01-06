
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
    ROW_NUMBER() OVER (ORDER BY a.Dated, a.ArticleNo, b.SubOperation_ID ) AS id, 
    a.MainDeptt_ID,
    a.ApplicableDate,
    a.BaseArticleNo,
    a.ArticleNo,
    a.ConversionFactor,
    a.Mnth,
    a.vno,
    a.Dated,
    b.SubOperation_ID,
    a.TotalSMV,
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
   ROW_NUMBER() OVER (ORDER BY ApparalProduction.Dated,ApparalProduction.ScrVoucher_No,ApparalProduction.Line_ID, ApparalProduction.BarCode, ApparalProduction.PONo, ApparalProduction.Item_ID,Coa31.FG_ArticleNo,Coa31.FG_Colour, Coa31.FG_BallSize) AS id,
	ApparalProduction.Dated, 
	ApparalProduction.ScrVoucher_No, 
	ApparalProduction.BarCode, 
	ApparalProduction.MainDeptt_ID, 
	ApparalProduction.PONo, 
	ApparalProduction.Item_ID,
	
	CASE ApparalProduction.Line_ID
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
		WHEN '034' THEN 'line-34' WHEN '34N' THEN 'line-34' WHEN '34' THEN 'line-34'
		WHEN '035' THEN 'line-35' WHEN '35N' THEN 'line-35' WHEN '35' THEN 'line-35'
		WHEN '036' THEN 'line-36' WHEN '36N' THEN 'line-36' WHEN '36' THEN 'line-36'
		WHEN '037' THEN 'line-37' WHEN '37N' THEN 'line-37' WHEN '37' THEN 'line-37'
		WHEN '038' THEN 'line-38' WHEN '38N' THEN 'line-38' WHEN '38' THEN 'line-38'
		WHEN '039' THEN 'line-39' WHEN '39N' THEN 'line-39' WHEN '39' THEN 'line-39'
		WHEN '040' THEN 'line-40' WHEN '40N' THEN 'line-40' WHEN '40' THEN 'line-40'
		WHEN '041' THEN 'line-41' WHEN '41N' THEN 'line-41' WHEN '41' THEN 'line-41'
    ELSE 'Other Line'
    END AS Line_ID,
	Coa31.Title, 
	Coa31.Model, 
	Coa31.FG_ArticleNo, 
	TD.MCombo Mcombo,
	TD.MColour FG_Colour,
	TD.ItemSize FG_Size,
	ApparalProduction.BundleNo,	
	ApparalProduction.Qty
	FROM 
		dbo.ApparalProduction
	LEFT JOIN
	dbo.Coa31
	ON 
		ApparalProduction.Item_ID = Coa31.id
	LEFT JOIN
	  ExportCPO_Det TD on ApparalProduction.PONo=TD.PONo and ApparalProduction.Item_ID=TD.Item_ID
WHERE
	ApparalProduction.MainDeptt_ID = 'DTS'
	AND ApparalProduction.Dated>='2025-06-01'
	AND Line_ID between '021' and '032' ;
GO


-- 4. Article
IF OBJECT_ID('dbo.Article', 'V') IS NOT NULL
    DROP VIEW dbo.Article;
GO

CREATE VIEW dbo.Article AS
select	id,
		title	as TIS_StyleDescription,
		model	as TIS_StyleCollection,
		BaseArticleNo,
		FG_ArticleNo,
		Category_ID,
		brand	as style_brand,
		FG_Season as style_season,
		FG_BallSize as TIS_StyleSize,
		FG_Colour   as TIS_StyleColour,
		CreationDate,
		PicEntryNo,
		FMaterial,
		ItemActive
	 FROM         dbo.Coa31

where FMaterial=1
GO

-- 5. hangerline_emp (MOST IMPORTANT ONE – FULLY FIXED)
IF OBJECT_ID('dbo.hangerline_emp', 'V') IS NOT NULL
    DROP VIEW dbo.hangerline_emp;
GO

CREATE VIEW dbo.hangerline_emp AS
SELECT
    REPLACE(e.ID, '-', '') AS ID,
	e.ID as EMP_ID,
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
		WHEN '034' THEN 'line-34' WHEN '34N' THEN 'line-34' WHEN '34' THEN 'line-34'
		WHEN '035' THEN 'line-35' WHEN '35N' THEN 'line-35' WHEN '35' THEN 'line-35'
		WHEN '036' THEN 'line-36' WHEN '36N' THEN 'line-36' WHEN '36' THEN 'line-36'
		WHEN '037' THEN 'line-37' WHEN '37N' THEN 'line-37' WHEN '37' THEN 'line-37'
		WHEN '038' THEN 'line-38' WHEN '38N' THEN 'line-38' WHEN '38' THEN 'line-38'
		WHEN '039' THEN 'line-39' WHEN '39N' THEN 'line-39' WHEN '39' THEN 'line-39'
		WHEN '040' THEN 'line-40' WHEN '40N' THEN 'line-40' WHEN '40' THEN 'line-40'
		WHEN '041' THEN 'line-41' WHEN '41N' THEN 'line-41' WHEN '41' THEN 'line-41'
    ELSE 'Other Line'
    END AS Line_Desc,

    CASE WHEN CHARINDEX('N', COALESCE(c.Line_ID, e.Line_ID)) > 0 THEN 'Night' ELSE 'Day' END AS Shift,

    e.Location_ID,
    e.Joindate,
		e.ResignDate,
		e.NICNew as NIC,
    e.add1,
    e.Mobile,
    case when e.ActiveStatus='Active' then 1 else 0 end as ActiveStatus,
    case when e.Male=1 then 'M' else 'F' end as Gender

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
where 
    Deptt_ID='1-06-13' and 
    Location_ID between '040' and '041' and 
    activestatus='active' ;
  
GO

PRINT 'All 5 views recreated successfully!';
