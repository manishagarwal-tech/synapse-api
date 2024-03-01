CREATE EXTERNAL TABLE company_borrowers
WITH(
    LOCATION = 'flatten-data/company_tagged/',
    DATA_SOURCE = [maneesh-synapse-demo_maneeshadls_dfs_core_windows_net],
    FILE_FORMAT = [SynapseParquetFormat]
)
AS 
(SELECT  
APN,Borrower,BuyerMailFullStreetAddress,DPID,LoanAmount,LoanDueDate,LoanTransactionType,OriginalDateOfContract,PartialLoanAmount,PID,PropertyCityName,PropertyFullStreetAddress,PropertyState,PropertyUnitNumber,PropertyUnitType,PropertyUseCode,PropertyZip4,PropertyZipCode,RcResidentialType,RcType,source
FROM dbo.deedFlattenTable
WHERE RcType = 'Company'
)
UNION ALL
(
SELECT
APN,Borrower,BuyerMailFullStreetAddress,DPID,LoanAmount,LoanDueDate,LoanTransactionType,OriginalDateOfContract,PartialLoanAmount,PID,PropertyCityName,PropertyFullStreetAddress,PropertyState,PropertyUnitNumber,PropertyUnitType,PropertyUseCode,PropertyZip4,PropertyZipCode,RcResidentialType,RcType,source
FROM dbo.samFlattenTable 
WHERE RcType = 'Company'
)




CREATE EXTERNAL TABLE individual_borrowers
WITH(
    LOCATION = 'flatten-data/individual_tagged/',
    DATA_SOURCE = [maneesh-synapse-demo_maneeshadls_dfs_core_windows_net],
    FILE_FORMAT = [SynapseParquetFormat]
)
AS 
(SELECT  
APN,Borrower,BuyerMailFullStreetAddress,DPID,LoanAmount,LoanDueDate,LoanTransactionType,OriginalDateOfContract,PartialLoanAmount,PID,PropertyCityName,PropertyFullStreetAddress,PropertyState,PropertyUnitNumber,PropertyUnitType,PropertyUseCode,PropertyZip4,PropertyZipCode,RcResidentialType,RcType,source
FROM dbo.deedFlattenTable
WHERE RcType = 'Individual'
)
UNION ALL
(
SELECT
APN,Borrower,BuyerMailFullStreetAddress,DPID,LoanAmount,LoanDueDate,LoanTransactionType,OriginalDateOfContract,PartialLoanAmount,PID,PropertyCityName,PropertyFullStreetAddress,PropertyState,PropertyUnitNumber,PropertyUnitType,PropertyUseCode,PropertyZip4,PropertyZipCode,RcResidentialType,RcType,source
FROM dbo.samFlattenTable 
WHERE RcType = 'Individual'
)


